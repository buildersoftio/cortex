# Branches, Merge and Dynamic Branching

Cortex.Streams can split a stream into named branches, let a branch accept only some items, merge branch outputs back into one stream, and add or remove branches while the stream runs.

## Table of Contents

- [Overview](#overview)
- [Conditional Branches](#conditional-branches)
- [Merging Branches](#merging-branches)
- [Dynamic Branching](#dynamic-branching)
- [Real-World Examples](#real-world-examples)
  - [E-Commerce Order Fulfilment](#e-commerce-order-fulfilment)
  - [Payment Fraud Signals](#payment-fraud-signals)
  - [Security Log Normalisation](#security-log-normalisation)
  - [IoT Sensor Monitoring with a Live Debug Tap](#iot-sensor-monitoring-with-a-live-debug-tap)
  - [Per-Tenant Webhook Subscriptions](#per-tenant-webhook-subscriptions)
- [Choosing Between AddBranch, FanOut and Merge](#choosing-between-addbranch-fanout-and-merge)
- [Pitfalls](#pitfalls)
- [Thread Safety and Ordering](#thread-safety-and-ordering)
- [Errors](#errors)
- [API Reference](#api-reference)

---

## Overview

| Feature | API | Result |
|---------|-----|--------|
| Branch | `AddBranch(name, config)` | Every item reaches the branch; the main pipeline continues after it. |
| Conditional branch | `AddBranch(name, condition, config)` | Only items matching `condition` enter the branch; the main pipeline still sees every item. |
| Merge | `Merge<TNext>(m => m.Branch(...))` | Items go through every (matching) branch and all branch outputs continue as one stream. |
| Runtime add | `stream.AddBranch<T>(name, [condition,] config)` | A branch is attached to a running stream. |
| Runtime remove | `stream.RemoveBranch(name)` | A branch is detached from a running stream. |

The short snippets in the next three sections use these declarations; the [real-world examples](#real-world-examples) further down declare their own.

```csharp
public record Customer(string Id, bool IsVip);
public record CustomerOrder(string Id, decimal Amount, Customer Customer);
public record Alert(string OrderId);
public record Reading(string SensorId, double Value);

public interface IAlertService { void Send(Alert alert); }
public interface IAuditLog { void Write(CustomerOrder order); }
public interface IOrderRepository { void Save(CustomerOrder order); }
public interface ICrm { void Notify(CustomerOrder order); }

// Variables used by the snippets
IAlertService alertService;
IAuditLog auditLog;
IOrderRepository repository;
ICrm crm;
```

## Conditional Branches

Pass a predicate before the branch configuration. Items that do not match skip the branch; the main pipeline is unaffected.

```csharp
var stream = StreamBuilder<CustomerOrder>.CreateNewStream("Orders")
    .Stream()
    .AddBranch("large-orders", order => order.Amount > 10_000, branch => branch
        .Map(order => new Alert(order.Id))
        .Sink(alert => alertService.Send(alert)))
    .AddBranch("audit", branch => branch
        .Sink(order => auditLog.Write(order)))
    .Sink(order => repository.Save(order))
    .Build();
```

The condition runs as a filter at the head of the branch, so it follows the stream's error handling: with `ErrorHandlingStrategy.Skip`, an item whose condition throws is skipped for that branch only.

## Merging Branches

`Merge<TNext>` sends each item through several branches and joins their outputs into one stream of `TNext`. Every branch must end with the same type. Operators chained after `Merge` receive every branch output.

```csharp
var stream = StreamBuilder<Reading>.CreateNewStream("Sensors")
    .Stream()
    .Merge<string>(merge => merge
        .Branch("all", b => b.Map(r => $"{r.SensorId}: {r.Value}"))
        .Branch("too-hot", r => r.Value > 80, b => b.Map(r => $"ALERT {r.SensorId}")))
    .Map(line => line.ToUpperInvariant())
    .Sink(line => Console.WriteLine(line))
    .Build();
```

- A branch configuration returns the builder at the end of the branch, such as `b => b.Map(...)`. It cannot end with `Sink`; the sink belongs after the merge.
- `b => b` is a pass-through branch: items reach the merge unchanged. This needs `TNext` to be the item type at the merge point.
- For one input item, branch outputs arrive in the order the branches were added. In the example above, a reading of 90 produces `"S1: 90"` and then `"ALERT S1"`.
- Merge branches appear in `stream.GetBranches()` and can be removed with `stream.RemoveBranch(name)`.
- State stores used inside merge branches or after the merge are found by `GetStateStoreByName` and `GetStateStoresByType`.

## Dynamic Branching

A running stream can gain and lose branches without being rebuilt.

```csharp
var stream = StreamBuilder<CustomerOrder>.CreateNewStream("Orders")
    .Stream()
    .AddBranch("audit", b => b.Sink(order => auditLog.Write(order)))
    .Sink(order => repository.Save(order))
    .Build();

stream.Start();

// Later: start sending VIP orders to a CRM, then stop
stream.AddBranch<CustomerOrder>("vip", order => order.Customer.IsVip, b => b
    .Sink(order => crm.Notify(order)));

stream.RemoveBranch("vip"); // true: the branch existed and was removed
```

### Where a runtime branch is attached

A runtime branch is attached to a **branch point**: a place where the stream was built with `AddBranch` or `FanOut`. The type argument `T` of `AddBranch<T>` selects it:

- The stream must have **exactly one** branch point whose items are of type `T`. Declare at least one branch at that point when you build the stream.
- If there is none, or more than one, `AddBranch<T>` throws `InvalidOperationException`.
- The fork inside a `Merge` is never chosen, because a branch added there would not end in the merge.

The new branch gets the stream's telemetry provider and error handling before it receives its first item.

### Branch names

Runtime branch names must be unique across the whole stream; adding a name that already exists throws `ArgumentException` and leaves the existing branch in place. `RemoveBranch(name)` removes the branch with that name wherever it is and returns `false` if there was none.

### What removal releases

`RemoveBranch` disposes the operators that belong only to the removed branch, such as window and stream-stream join operators, which stops their timers. Windows still open in the removed branch are dropped, not emitted. When the branch belongs to a `Merge`, the merge and the operators after it are shared with the other branches and keep running.

## Real-World Examples

Each example declares the records and service interfaces it uses, so you can paste it into a project that references `Cortex.Streams` and plug in your own implementations.

The examples use these namespaces:

```csharp
using Cortex.States;                  // InMemoryStateStore
using Cortex.Streams;
using Cortex.Streams.ErrorHandling;   // StreamExecutionOptions, ErrorHandlingStrategy
```

### E-Commerce Order Fulfilment

Every order triggers a confirmation email. Standard and express orders take different routes through fulfilment, but both routes end in the same work item, a pick task for the warehouse. `Merge` joins the two routes, so the warehouse queue is written in one place.

```csharp
public record OrderLine(string Sku, int Quantity, string Warehouse);
public record Order(string Id, string CustomerId, bool IsExpress, IReadOnlyList<OrderLine> Lines);
public record PickTask(string OrderId, string Sku, int Quantity, string Warehouse, string Priority);

public interface IEmailService { void SendConfirmation(string customerId, string orderId); }
public interface IWarehouseQueue { void Enqueue(PickTask task); }

public class FulfilmentPipeline
{
    private readonly IEmailService _email;
    private readonly IWarehouseQueue _warehouse;

    public FulfilmentPipeline(IEmailService email, IWarehouseQueue warehouse)
    {
        _email = email;
        _warehouse = warehouse;
    }

    public IStream<Order> Create()
    {
        return StreamBuilder<Order>.CreateNewStream("Fulfilment")
            .Stream()
            // A side effect for every order; it does not change what flows on
            .AddBranch("confirmation-email", b => b
                .Sink(order => _email.SendConfirmation(order.CustomerId, order.Id)))
            // Two routes that produce the same kind of work item
            .Merge<PickTask>(routes => routes
                .Branch("standard", order => !order.IsExpress, b => b
                    .FlatMap(order => order.Lines.Select(line =>
                        new PickTask(order.Id, line.Sku, line.Quantity, line.Warehouse, "standard"))))
                .Branch("express", order => order.IsExpress, b => b
                    .FlatMap(order => order.Lines.Select(line =>
                        new PickTask(order.Id, line.Sku, line.Quantity, "express-hub", "express")))))
            .Sink(task => _warehouse.Enqueue(task))
            .Build();
    }
}
```

An express order with two lines produces two pick tasks for `express-hub`; a standard order produces one pick task per line, each for the line's own warehouse. The two conditions cover every order, so no order is lost at the merge.

### Payment Fraud Signals

Each fraud rule is a conditional merge branch that emits a `RiskSignal` only when it fires. The merged signals go to the fraud team's case queue, and `AggregateSilently` keeps a running risk score per payment in a state store that other services can read.

```csharp
public record Payment(string Id, decimal Amount, string Country, string CardCountry, bool IsNewCard);
public record RiskSignal(string PaymentId, string Rule, int Points);

public interface IFraudCaseQueue { void Add(RiskSignal signal); }

public class FraudSignalsPipeline
{
    public const string ScoreStoreName = "payment-risk-scores";

    private readonly IFraudCaseQueue _cases;

    public FraudSignalsPipeline(IFraudCaseQueue cases)
    {
        _cases = cases;
    }

    public IStream<Payment> Create()
    {
        return StreamBuilder<Payment>.CreateNewStream("FraudSignals")
            .Stream()
            .Merge<RiskSignal>(rules => rules
                .Branch("large-amount", p => p.Amount > 5_000m, b => b
                    .Map(p => new RiskSignal(p.Id, "large-amount", 40)))
                .Branch("foreign-card", p => p.Country != p.CardCountry, b => b
                    .Map(p => new RiskSignal(p.Id, "foreign-card", 30)))
                .Branch("new-card", p => p.IsNewCard, b => b
                    .Map(p => new RiskSignal(p.Id, "new-card", 20))))
            // Running score per payment; the signal itself flows on unchanged
            .AggregateSilently<string, int>(s => s.PaymentId, (score, s) => score + s.Points, ScoreStoreName)
            .Sink(signal => _cases.Add(signal))
            .Build();
    }
}
```

Reading the score:

```csharp
var stream = new FraudSignalsPipeline(caseQueue).Create();
stream.Start();
stream.Emit(new Payment("pay-42", 7_500m, "AT", "US", IsNewCard: true));

var scores = stream.GetStateStoreByName<InMemoryStateStore<string, int>>(FraudSignalsPipeline.ScoreStoreName);
int score = scores.Get("pay-42"); // 90: large amount + foreign card + new card
```

A payment that triggers no rule produces no signal at all, which is what you want here. Adding a rule is one more `.Branch(...)` line.

### Security Log Normalisation

Logs arrive from several sources in different formats. Each source gets a conditional merge branch that parses its format into one `SecurityEvent` type, and the merged events go to the SIEM. A catch-all branch keeps lines from unknown sources instead of dropping them, and `ErrorHandlingStrategy.Skip` makes a malformed line skip only its own branch instead of failing the stream.

```csharp
public record RawLog(string Source, string Line, DateTime ReceivedAt);
public record SecurityEvent(string Source, string Action, string User, DateTime Timestamp);

public interface ISiemClient { void Send(SecurityEvent securityEvent); }

public class SecurityLogPipeline
{
    private readonly ISiemClient _siem;

    public SecurityLogPipeline(ISiemClient siem)
    {
        _siem = siem;
    }

    public IStream<RawLog> Create()
    {
        return StreamBuilder<RawLog>.CreateNewStream("SecurityLogs")
            .WithErrorHandling(new StreamExecutionOptions { ErrorHandlingStrategy = ErrorHandlingStrategy.Skip })
            .Stream()
            .Merge<SecurityEvent>(sources => sources
                // nginx access log: "<ip> <user> <method> <path>"
                .Branch("nginx", log => log.Source == "nginx", b => b
                    .Map(ParseNginx))
                // auth service: "<action>:<user>", without its health checks
                .Branch("auth", log => log.Source == "auth", b => b
                    .Map(ParseAuth)
                    .Filter(e => e.Action != "heartbeat"))
                // Everything else is forwarded as unparsed, never silently dropped
                .Branch("unknown", log => log.Source != "nginx" && log.Source != "auth", b => b
                    .Map(log => new SecurityEvent(log.Source, "unparsed", null, log.ReceivedAt))))
            .Sink(e => _siem.Send(e))
            .Build();
    }

    private static SecurityEvent ParseNginx(RawLog log)
    {
        var parts = log.Line.Split(' ');
        return new SecurityEvent("nginx", $"{parts[2]} {parts[3]}", parts[1], log.ReceivedAt);
    }

    private static SecurityEvent ParseAuth(RawLog log)
    {
        var parts = log.Line.Split(':');
        return new SecurityEvent("auth", parts[0], parts[1], log.ReceivedAt);
    }
}
```

`"10.0.0.7 alice GET /admin"` from nginx becomes `SecurityEvent("nginx", "GET /admin", "alice", …)`. A truncated nginx line throws inside `ParseNginx`, is skipped, and the next log flows normally.

### IoT Sensor Monitoring with a Live Debug Tap

Every reading is checked for overheating and averaged per sensor over a tumbling window. While investigating an incident, support can switch on a debug tap for one site and switch it off again, without restarting the pipeline.

```csharp
public record SensorReading(string SensorId, string Site, double Temperature, DateTime Timestamp);
public record SensorAverage(string SensorId, DateTime WindowStart, double AverageTemperature);

public interface IPager { void Page(string message); }
public interface ITimeSeriesStore { void Write(SensorAverage average); }
public interface IDebugLog { void Write(SensorReading reading); }

public class SensorTelemetry
{
    private readonly IStream<SensorReading> _stream;
    private readonly IDebugLog _debugLog;

    public SensorTelemetry(IPager pager, ITimeSeriesStore timeSeries, IDebugLog debugLog, TimeSpan averageWindow)
    {
        _debugLog = debugLog;
        _stream = StreamBuilder<SensorReading>.CreateNewStream("SensorTelemetry")
            .Stream()
            .AddBranch("overheat", r => r.Temperature > 90, b => b
                .Sink(r => pager.Page(FormattableString.Invariant($"{r.SensorId} at {r.Site} reports {r.Temperature:F1} C"))))
            .AddBranch("averages", b => b
                .TumblingWindow(r => r.SensorId, r => r.Timestamp, averageWindow)
                .Map(w => new SensorAverage(w.Key, w.WindowStart, w.Items.Average(r => r.Temperature)))
                .Sink(average => timeSeries.Write(average)))
            .Build();
        _stream.Start();
    }

    public void Publish(SensorReading reading) => _stream.Emit(reading);

    // Attaches to the branch point declared by "overheat" and "averages"
    public void StartDebugTap(string site) =>
        _stream.AddBranch<SensorReading>($"debug-{site}", r => r.Site == site, b => b
            .Sink(r => _debugLog.Write(r)));

    public bool StopDebugTap(string site) => _stream.RemoveBranch($"debug-{site}");
}
```

- Readings need UTC timestamps close to the current time: the window closes when the wall clock passes the window's end. For readings that arrive out of order or are replayed, add `WithWatermarks` before the branches; see [Event Time and Watermarks](Event-Time-and-Watermarks.md).
- Starting a tap for a site that already has one throws `ArgumentException`, because the branch name is taken. `StopDebugTap` returns `false` when no tap was running.

### Per-Tenant Webhook Subscriptions

A SaaS platform lets each tenant subscribe to order events through webhooks. Each subscription is a runtime branch with a condition on the tenant and the event status. Subscribing and unsubscribing never restarts the stream, and events keep flowing to every other subscriber while a subscription changes.

```csharp
public record OrderEvent(string TenantId, string OrderId, string Status, DateTime OccurredAt);
public record WebhookDelivery(string SubscriptionId, Uri Endpoint, OrderEvent Event);

public interface IEventStore { void Append(OrderEvent orderEvent); }
public interface IWebhookOutbox { void Enqueue(WebhookDelivery delivery); }

public class OrderWebhooks
{
    private const string Prefix = "webhook-";

    private readonly IStream<OrderEvent> _stream;
    private readonly IWebhookOutbox _outbox;

    public OrderWebhooks(IEventStore eventStore, IWebhookOutbox outbox)
    {
        _outbox = outbox;
        _stream = StreamBuilder<OrderEvent>.CreateNewStream("OrderWebhooks")
            .Stream()
            // Also declares the branch point that subscriptions attach to
            .AddBranch("event-store", b => b.Sink(e => eventStore.Append(e)))
            .Build();
        _stream.Start();
    }

    public void Publish(OrderEvent orderEvent) => _stream.Emit(orderEvent);

    public void Subscribe(string subscriptionId, string tenantId, Uri endpoint, params string[] statuses)
    {
        _stream.AddBranch<OrderEvent>(
            Prefix + subscriptionId,
            e => e.TenantId == tenantId && (statuses.Length == 0 || statuses.Contains(e.Status)),
            b => b
                .Map(e => new WebhookDelivery(subscriptionId, endpoint, e))
                .Sink(delivery => _outbox.Enqueue(delivery)));
    }

    public bool Unsubscribe(string subscriptionId) => _stream.RemoveBranch(Prefix + subscriptionId);

    public IEnumerable<string> ActiveSubscriptions() =>
        _stream.GetBranches().Keys
            .Where(name => name.StartsWith(Prefix))
            .Select(name => name.Substring(Prefix.Length));
}
```

- The sink only enqueues the delivery into an outbox. A sink runs on the thread that emitted the event, so a slow HTTP call there would slow every publisher; a separate worker sends the outbox.
- Deterministic branch names (`webhook-<id>`) let `Unsubscribe` and `ActiveSubscriptions` find the branches without a separate registry of subscriptions.

## Choosing Between AddBranch, FanOut and Merge

| You need | Use |
|----------|-----|
| Side effects next to the main pipeline (audit, notifications, metrics) | `AddBranch`, with a condition if only some items qualify |
| The same item delivered to several sinks, with little processing per sink | `FanOut` (see [FanOut - Multiple Sinks](FanOut-Multiple-Sinks.md)) |
| Several routes or rules whose results continue as one stream | `Merge` |
| Consumers that come and go while the stream runs | `stream.AddBranch<T>` and `stream.RemoveBranch`, attached to a branch point declared with `AddBranch` or `FanOut` |
| Enrichment with reference data | a [Stream-Table Join](Stream-Table-Joins.md) before the branches |
| Correlation of two streams in a time window | a [Stream-Stream Join](Stream-Stream-Joins.md) |

## Pitfalls

- **Items can vanish at a merge.** An item that matches no merge branch produces no output. Make the conditions cover every case, or add a catch-all branch as in [Security Log Normalisation](#security-log-normalisation).
- **No sink inside a merge branch.** A merge branch must return its builder so its output can reach the merge. Put sinks after the merge, or side effects in an `AddBranch` before it.
- **Runtime branches need a branch point.** `stream.AddBranch<T>` only attaches where the stream was built with `AddBranch` or `FanOut` for `T`. Declare one there, even a small audit or metrics branch.
- **Sinks after a merge may run concurrently.** When several threads emit at once, the operators after a merge are called from all of them. Keep those sinks thread-safe.

## Thread Safety and Ordering

- `AddBranch<T>` and `RemoveBranch` can be called from any thread while items are being emitted, including from inside a sink of the same stream.
- Each branch point keeps an immutable snapshot of its branches. An item reads the snapshot once when it reaches the branch point, so:
  - a branch added or removed takes effect for items that reach the branch point **after** the call returns;
  - an item that is already being processed finishes on the branches it started with, so it can still reach a branch that is being removed.
- Processing takes no locks; only branch changes are serialised. `Merge` adds no locking either: when you emit from several threads at once, operators after the merge are called concurrently, as every other operator in the pipeline is.

## Errors

| Call | Exception | When |
|------|-----------|------|
| `AddBranch` (builder or runtime) | `ArgumentException` | The name is null or empty. |
| `AddBranch` (builder or runtime) | `ArgumentNullException` | `config`, or `condition` in the conditional overload, is null. |
| `AddBranch` (builder or runtime) | `InvalidOperationException` | The branch configuration adds no operator. |
| `stream.AddBranch<T>` | `ArgumentException` | A branch with this name already exists in the stream. |
| `stream.AddBranch<T>` | `InvalidOperationException` | No branch point, or more than one, carries items of type `T`. |
| `Merge` | `ArgumentNullException` | `config` is null. |
| `Merge` | `InvalidOperationException` | No branch was added. |
| `Branch` (in a merge) | `ArgumentException` | The name is null, empty or already used in this merge, or the configuration returned a builder it did not get from its argument. |
| `Branch` (in a merge) | `ArgumentNullException` | `config` or `condition` is null. |

A failed `Merge` leaves the pipeline unchanged.

## API Reference

### IStreamBuilder&lt;TIn, TCurrent&gt;

```csharp
IStreamBuilder<TIn, TCurrent> AddBranch(string name, Action<IBranchStreamBuilder<TIn, TCurrent>> config);
IStreamBuilder<TIn, TCurrent> AddBranch(string name, Func<TCurrent, bool> condition, Action<IBranchStreamBuilder<TIn, TCurrent>> config);
IStreamBuilder<TIn, TNext> Merge<TNext>(Action<IMergeBuilder<TIn, TCurrent, TNext>> config);
```

### IMergeBuilder&lt;TIn, TCurrent, TNext&gt;

```csharp
IMergeBuilder<TIn, TCurrent, TNext> Branch(string name, Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config);
IMergeBuilder<TIn, TCurrent, TNext> Branch(string name, Func<TCurrent, bool> condition, Func<IBranchStreamBuilder<TIn, TCurrent>, IBranchStreamBuilder<TIn, TNext>> config);
```

### IStream&lt;TIn&gt;

```csharp
void AddBranch<TBranch>(string name, Action<IBranchStreamBuilder<TIn, TBranch>> config);
void AddBranch<TBranch>(string name, Func<TBranch, bool> condition, Action<IBranchStreamBuilder<TIn, TBranch>> config);
bool RemoveBranch(string name);
IReadOnlyDictionary<string, IBranchInfo> GetBranches();
```

`GetBranches()` lists every branch in the stream, including branches added at runtime, merge branches and branches declared before a type-changing operator such as `Map`.

### Telemetry

`MergeOperator<T>` reports `merge_operator_processed_{T}` (counter) and `merge_operator_processing_time_{T}` (histogram), and traces `MergeOperator.Process` spans, like the other operators.

## Related Documentation

- [FanOut - Multiple Sinks](FanOut-Multiple-Sinks.md)
- [Stream-Table Joins](Stream-Table-Joins.md)
- [Stream-Stream Windowed Joins](Stream-Stream-Joins.md)
- [Stream Performance and Async Processing](stream-performance.md)
