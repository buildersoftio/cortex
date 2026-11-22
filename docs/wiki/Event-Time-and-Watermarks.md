# Event Time and Watermarks in Cortex.Streams

By default every window and stream-stream join in Cortex.Streams works in **processing time**: a window closes when the wall clock passes its end, and a join expires events against `DateTime.UtcNow`. That is fine for live data that arrives on time, but it gives wrong answers when:

- events arrive **out of order** (mobile clients, gateways that batch, several partitions),
- you **replay history** from Kafka, Pulsar or an archive, where every timestamp is "old",
- the question is about **when things happened**, not when they were received ("orders placed between 10:00 and 11:00").

Calling `WithWatermarks` switches a stream to **event time**: windows and joins after it are driven by the timestamps in the data. Streams without `WithWatermarks` behave exactly as before.

---

## Quick Start

```csharp
using Cortex.Streams;
using Cortex.Streams.Watermarks;

var stream = StreamBuilder<SensorReading>
    .CreateNewStream("sensor-averages")
    .Stream()
    .WithWatermarks(r => r.MeasuredAt, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(10)))
    .TumblingWindow(r => r.SensorId, r => r.MeasuredAt, TimeSpan.FromMinutes(1))
    .Sink(window => Console.WriteLine($"{window.Key} {window.WindowStart:HH:mm}: {window.Items.Count} readings"))
    .Build();
```

Readings may arrive up to 10 seconds out of order, and each minute is emitted once, after the readings for that minute have had 10 seconds to arrive.

---

## How It Works

### The watermark

A **watermark** is the stream's notion of "how far event time has got". `WithWatermarks` tracks the newest event timestamp it has seen and computes:

```
watermark = newest timestamp - MaxOutOfOrderness
```

It says: every event older than the watermark has probably arrived. The watermark never moves backwards; an out-of-order event does not lower it.

| Strategy | Watermark | Use when |
|----------|-----------|----------|
| `WatermarkStrategy.BoundedOutOfOrderness(delay)` | newest timestamp − `delay` | Events arrive at most `delay` late (the usual case) |
| `WatermarkStrategy.ForMonotonousTimestamps()` | newest timestamp | Timestamps never go backwards (one ordered partition) |

Strategies are immutable; each `With...` method returns a new one:

| Method | Default | Effect |
|--------|---------|--------|
| `WithAllowedLateness(TimeSpan)` | zero | How long after a window's end it still accepts events and fires again |
| `OnLateEvent(Action<LateEvent>)` | none | Called for every event dropped as too late |
| `WithIdleTimeout(TimeSpan)` | none | Keeps the watermark moving when no events arrive (see [Idle sources](#idle-sources)) |
| `WithStateStore(IDataStore<string, long>)` | none | Saves and restores the watermark (see [Restarts](#restarts)) |
| `WithClock(Func<DateTime>)` | `DateTime.UtcNow` | The wall clock used for idleness and saving, for example in tests |

Event timestamps must be **UTC**.

### Where to call it

- Call `WithWatermarks` **once per path, before the first window or join**. Operators before it keep processing time; every window and join after it, including those inside `AddBranch`, `FanOut` and `Merge` branches and branches added at runtime, switch to event time.
- A second `WithWatermarks` on the same path throws `InvalidOperationException` when the stream is built.
- Map, Filter, FlatMap and sinks are not affected; the watermark is tracked beside the items, not sent through the pipeline.
- When an item advances the watermark, the item reaches every operator first, and the operators then see the new watermark in pipeline order. A window that feeds another window therefore emits its results before the downstream window closes.

### Windows

| Window | Fires | Kept until | An event is late when |
|--------|-------|------------|-----------------------|
| `TumblingWindow`, `SlidingWindow` `[start, end)` | watermark ≥ `end` | watermark ≥ `end + AllowedLateness` | `end + AllowedLateness ≤ watermark` |
| `SessionWindow` | watermark > last event + gap | watermark > last event + gap + `AllowedLateness` | `timestamp + gap + AllowedLateness < watermark` |

- **Allowed lateness:** an event that arrives after its window fired but within the allowed lateness is added, and the window fires again with all its items. Downstream code should treat the newer result for the same key and window as a replacement.
- **Late events** are dropped, not put into a new window. They are counted and passed to `OnLateEvent`. A sliding-window event that is late for some of its windows still goes into the others and is reported once.
- **Sessions:** each event covers `[timestamp, timestamp + gap]`, and events whose intervals overlap form one session. An out-of-order event can extend a session backwards or merge two sessions, and a key can have several open sessions at once.
- The wall-clock timer of an event-time window no longer closes it.

### Advanced windows and triggers

`AdvancedTumblingWindow`, `AdvancedSlidingWindow` and `AdvancedSessionWindow` pass the **watermark** to their triggers wherever they would pass the processing time:

- The default `EventTimeTrigger` fires when the watermark reaches the window end.
- `CountTrigger` fires on its count and purges when the watermark reaches the end.
- Interval triggers (`TriggerOnProcessingTime`, `WithEarlyTrigger`) measure their interval in **event time**: an early trigger of 10 seconds fires for every 10 seconds the watermark advances.
- An allowed-late event fires the window at once, with `EmissionType` `Late`.
- `WindowConfiguration.AllowedLateness` and `OnLateEvent`, when set, take precedence over the strategy's.

### Stream-stream joins

A `StreamStreamJoinOperator` downstream of `WithWatermarks` works in event time:

- Two events match when their timestamps are **less than `WindowSize` apart**, in either order, however old they are.
- The left watermark comes from the stream. The right watermark is tracked from the timestamps passed to `ProcessRight`, with the same strategy. The join uses the **smaller** of the two, so nothing expires until both sides have moved forward.
- An event expires once that watermark reaches `timestamp + WindowSize + GracePeriod`. Unmatched events of left, right and outer joins are then emitted, on the thread that advanced the watermark.
- An event that arrives after its expiry point is dropped and reported as late.
- With an idle timeout, a right side that has had no events for that long stops holding the left side back. This is checked every `CleanupInterval`.

### Late events

`LateEvent` describes a dropped event:

| Property | Meaning |
|----------|---------|
| `Item` | The dropped item, as the window or join received it |
| `Timestamp` | Its event timestamp |
| `Watermark` | The watermark when it arrived |
| `OperatorName` | The operator that dropped it |

An exception thrown by the handler is written to `System.Diagnostics.Trace` and ignored, so a failing handler cannot stop the stream.

### Idle sources

With only event time, a window closes when a *later* event arrives. If a source goes quiet, the last windows stay open. `WithIdleTimeout(t)` fixes this: after `t` without events, the watermark keeps advancing **at wall-clock speed from where it stopped**.

- Idleness is checked on a timer every quarter of the timeout (at most once a second), so the advance can start up to one check later.
- The idle clock pauses while the stream is stopped.
- Advancing from where it stopped, rather than jumping to "now", keeps a replay of old data safe: windows close one by one, as if time went on.

### Restarts

`WithStateStore(store)` saves the watermark (under the keys `WatermarkStrategy.WatermarkStateKey` and `MaxTimestampStateKey`) at most once a second while it advances and when the stream stops or is disposed, and restores it when the stream is built again. The store is listed in `GetStateStores()`. Use one store, or one store name, per `WithWatermarks`.

Only the **watermark** is restored. Windows and join buffers that were open when the process stopped are not, so their items are lost unless your source replays them.

---

## Real-World Examples

The examples use these namespaces:

```csharp
using Cortex.States;
using Cortex.Streams;
using Cortex.Streams.Operators;
using Cortex.Streams.Operators.Joins;
using Cortex.Streams.Operators.Windows;
using Cortex.Streams.Watermarks;
```

### Per-Minute Sensor Averages from Batching Gateways

Gateways buffer readings and send them in batches, so readings arrive up to 10 seconds out of order. Each minute must contain the readings *measured* in that minute. A gateway that reconnects within a minute still corrects the average; anything later is logged.

```csharp
public sealed record SensorReading(string SensorId, DateTime MeasuredAt, double Celsius);
public sealed record MinuteAverage(string SensorId, DateTime Minute, double AverageCelsius, int Readings);

public interface ITemperatureDashboard
{
    // Called again for a minute that late readings changed: overwrite by (SensorId, Minute)
    void Publish(MinuteAverage average);
}

public interface ILateReadingLog
{
    void Record(SensorReading reading, DateTime watermark);
}

public sealed class SensorAverages
{
    private readonly IStream<SensorReading> _stream;

    public SensorAverages(ITemperatureDashboard dashboard, ILateReadingLog lateLog)
    {
        var strategy = WatermarkStrategy
            .BoundedOutOfOrderness(TimeSpan.FromSeconds(10))
            .WithAllowedLateness(TimeSpan.FromMinutes(1))
            .OnLateEvent(late => lateLog.Record((SensorReading)late.Item, late.Watermark));

        _stream = StreamBuilder<SensorReading>
            .CreateNewStream("sensor-averages")
            .Stream()
            .WithWatermarks(r => r.MeasuredAt, strategy)
            .TumblingWindow(r => r.SensorId, r => r.MeasuredAt, TimeSpan.FromMinutes(1))
            .Map(w => new MinuteAverage(w.Key, w.WindowStart, w.Items.Average(r => r.Celsius), w.Items.Count))
            .Sink(dashboard.Publish)
            .Build();
        _stream.Start();
    }

    public void OnReading(SensorReading reading) => _stream.Emit(reading);
}
```

- A minute is published once the watermark passes its end, that is when a reading 10 seconds after the minute has arrived.
- A reading for that minute arriving within the next minute publishes it again with every reading so far.
- After that, the reading goes to `ILateReadingLog`.

### User Sessions from a Replayed Click Log

At startup, a week of page views is replayed from the archive, then live views follow. Sessions end after 30 minutes without activity. Processing time would put the whole replayed week into one session per user; event time rebuilds the real sessions. The idle timeout closes the last sessions when live traffic is quiet, for example at night.

```csharp
public sealed record PageView(string UserId, string Page, DateTime ViewedAt);
public sealed record UserSession(string UserId, DateTime Start, DateTime End, int Pages);

public interface ISessionStore
{
    void Save(UserSession session);
}

public sealed class UserSessions
{
    private readonly IStream<PageView> _stream;

    public UserSessions(ISessionStore store)
    {
        var strategy = WatermarkStrategy
            .BoundedOutOfOrderness(TimeSpan.FromSeconds(30))
            .WithIdleTimeout(TimeSpan.FromMinutes(2));

        _stream = StreamBuilder<PageView>
            .CreateNewStream("user-sessions")
            .Stream()
            .WithWatermarks(v => v.ViewedAt, strategy)
            .SessionWindow(v => v.UserId, v => v.ViewedAt, TimeSpan.FromMinutes(30))
            .Map(w => new UserSession(w.Key, w.WindowStart, w.WindowEnd, w.Items.Count))
            .Sink(store.Save)
            .Build();
        _stream.Start();
    }

    public void Replay(IEnumerable<PageView> archivedViews)
    {
        foreach (var view in archivedViews)
            _stream.Emit(view);
    }

    public void OnLiveView(PageView view) => _stream.Emit(view);

    public void Stop() => _stream.Stop();
}
```

- `WindowEnd` of a session is the last view plus the 30-minute gap.
- After two quiet minutes the watermark advances by itself from the last view's time, so the sessions still open close half an hour (of wall-clock time) later.

### Hourly API Usage That Survives Restarts

API calls are billed per customer and hour. Calls are reported by several gateways and arrive up to a minute late. When the service restarts, it must not bill an hour twice: calls older than the watermark saved before the restart are reported as late instead of opening the hour again.

```csharp
public sealed record ApiCall(string CustomerId, string Endpoint, DateTime CalledAt);
public sealed record HourlyUsage(string CustomerId, DateTime Hour, int Calls);

public interface IBillingLedger
{
    void Add(HourlyUsage usage);
    void Investigate(ApiCall call);
}

public sealed class UsageBilling
{
    private readonly IStream<ApiCall> _stream;

    // Use a durable store in production, for example a RocksDbStateStore<string, long>
    public UsageBilling(IBillingLedger ledger, IDataStore<string, long> watermarkStore)
    {
        var strategy = WatermarkStrategy
            .BoundedOutOfOrderness(TimeSpan.FromMinutes(1))
            .WithStateStore(watermarkStore)
            .OnLateEvent(late => ledger.Investigate((ApiCall)late.Item));

        _stream = StreamBuilder<ApiCall>
            .CreateNewStream("usage-billing")
            .Stream()
            .WithWatermarks(c => c.CalledAt, strategy)
            .TumblingWindow(c => c.CustomerId, c => c.CalledAt, TimeSpan.FromHours(1))
            .Map(w => new HourlyUsage(w.Key, w.WindowStart, w.Items.Count))
            .Sink(ledger.Add)
            .Build();
        _stream.Start();
    }

    public void OnCall(ApiCall call) => _stream.Emit(call);

    public void Stop() => _stream.Stop();
}
```

- `Stop()` saves the watermark; the next `UsageBilling` built on the same store starts from it.
- The hour that was open at the restart is not restored. Replay its calls from your source, or bill it from the raw log.

### Ad Click Attribution over Last Week's Logs

Impressions and clicks are joined to attribute clicks to the ad that was shown, within 30 minutes. The job runs over last week's logs. In processing time every event would already be "expired"; in event time the join matches by the distance between the timestamps.

```csharp
public sealed record Impression(string AdId, string UserId, DateTime ShownAt);
public sealed record Click(string AdId, string UserId, DateTime ClickedAt);
public sealed record Attribution(string AdId, string UserId, DateTime ShownAt, DateTime? ClickedAt);

public interface IAttributionReport
{
    void Add(Attribution attribution);
}

public sealed class ClickAttribution
{
    private readonly IStream<Impression> _impressions;
    private readonly StreamStreamJoinOperator<Impression, Click, string, Attribution> _join;

    public ClickAttribution(IAttributionReport report)
    {
        _join = new StreamStreamJoinOperator<Impression, Click, string, Attribution>(
            i => $"{i.AdId}:{i.UserId}",
            c => $"{c.AdId}:{c.UserId}",
            i => i.ShownAt,
            c => c.ClickedAt,
            // Left join: unmatched impressions arrive with click == null
            (impression, click) => new Attribution(impression.AdId, impression.UserId, impression.ShownAt, click?.ClickedAt),
            StreamJoinConfiguration.LeftJoin(TimeSpan.FromMinutes(30)));

        _impressions = StreamBuilder<Impression>
            .CreateNewStream("click-attribution")
            .Stream()
            .WithWatermarks(i => i.ShownAt, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromMinutes(5)))
            .JoinStream(_join)
            .Sink(report.Add)
            .Build();
        _impressions.Start();
    }

    public void OnImpression(Impression impression) => _impressions.Emit(impression);

    public void OnClick(Click click) => _join.ProcessRight(click);

    public void Stop()
    {
        _impressions.Stop();
        _join.Dispose();
    }
}
```

- Each click within 30 minutes of an impression of the same ad and user produces an `Attribution` with its `ClickedAt`.
- An impression without a click is reported with `ClickedAt == null` once both the impression log and the click log have passed `ShownAt` plus 30 minutes.
- The match is symmetric: a click logged slightly *before* its impression (clock skew between servers) still matches.

---

## Choosing the Settings

| Setting | Too small | Too large |
|---------|-----------|-----------|
| `MaxOutOfOrderness` | Events reported as late | Results are delayed by this much |
| `AllowedLateness` | Late corrections are lost | Windows stay in memory longer and fire more often |
| `WithIdleTimeout` | Windows close during short pauses, later events become late | Results wait long when a source is quiet |

---

## Limits

- **One watermark per path.** A stream merges branches that share one watermark; there is no per-partition or per-branch watermark, and no watermark alignment between sources.
- **Ordering.** With `Emit` from one thread, or buffered processing with one consumer, items keep their order. With several consumers or concurrent `EmitAsync` calls, items can be reordered, and `MaxOutOfOrderness` must cover that too.
- **Checkpoints.** Only the watermark is saved. Cortex.Streams has no coordinated checkpoint of window and join state.
- **No late-event stream.** Late events go to a callback, not to a separate stream.
- **Cost.** Tracking the watermark costs about 10 ns per item: 5-9% on top of a tumbling window, but 25-30% on top of a trivial Map → Filter → Sink pipeline, measured with `tools/Cortex.Streams.WatermarkBench` on in-order data.
- **Interface change.** `IStreamBuilder` has a new member, `WithWatermarks`. Code that implements `IStreamBuilder` itself must add it.

## See Also

- [Branch and Merge](Branch-and-Merge.md)
- [Stream-Stream Joins](Stream-Stream-Joins.md)
- [Stream-Table Joins](Stream-Table-Joins.md)
