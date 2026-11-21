# Stream Performance & Async Processing

This guide covers the performance features in Cortex.Streams: buffered async processing, backpressure handling and high-throughput configurations.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration Options](#configuration-options)
- [Backpressure Strategies](#backpressure-strategies)
- [Emit Methods](#emit-methods)
- [Monitoring & Statistics](#monitoring--statistics)
- [Preset Configurations](#preset-configurations)
- [Best Practices](#best-practices)
- [Migration Guide](#migration-guide)
- [API Reference](#api-reference)
- [Complete Example](#complete-example)

---

## Overview

Cortex.Streams provides optional performance features for high-throughput scenarios:

| Feature | Description |
|---------|-------------|
| **Buffered processing** | A bounded internal buffer, drained by background consumer tasks |
| **Backpressure handling** | Configurable behavior when the buffer is full |
| **Async emission** | `EmitAsync` and `EmitAndForget` return once the item is buffered |
| **Batched reads** | Consumers read several buffered items at a time |
| **Parallel consumers** | Several consumer tasks run the pipeline concurrently |
| **Buffer statistics** | Real-time view of the buffer |

### Key Benefits

- **Non-blocking emission**: producers do not wait for the pipeline to finish.
- **Backpressure control**: decide what happens when producers are faster than the pipeline.
- **Higher throughput**: parallel consumers process several items at once.
- **Backward compatible**: everything is opt-in; existing code works unchanged.

The examples on this page use these namespaces:

```csharp
using Cortex.Streams;
using Cortex.Streams.ErrorHandling;   // StreamExecutionOptions
using Cortex.Streams.Performance;     // StreamPerformanceOptions, BackpressureStrategy, BufferFullException
```

---

## Quick Start

### Basic Stream (Default Behavior)

Without performance options, a stream processes each item synchronously:

```csharp
var stream = StreamBuilder<int>.CreateNewStream("BasicStream")
    .Stream()
    .Map(x => x * 2)
    .Sink(Console.WriteLine)
    .Build();

stream.Start();
stream.Emit(42);             // Blocks until the sink has run
await stream.EmitAsync(42);  // Runs the pipeline on the thread pool and waits for it
stream.Stop();
```

### Buffered Async Stream (High Performance)

Enable buffered processing for non-blocking emission:

```csharp
var stream = StreamBuilder<int>.CreateNewStream("FastStream")
    .WithPerformanceOptions(new StreamPerformanceOptions
    {
        EnableBufferedProcessing = true,
        BufferCapacity = 10_000,
        BackpressureStrategy = BackpressureStrategy.Block
    })
    .Stream()
    .Map(x => x * 2)
    .Sink(x => Console.WriteLine(x))
    .Build();

stream.Start();

// Returns as soon as the item is buffered
stream.EmitAndForget(42);

// Awaits buffering, not processing
await stream.EmitAsync(42);

// Graceful shutdown: waits for the buffer to drain
await stream.StopAsync();
```

`Emit` keeps its synchronous behavior even when buffering is enabled: it runs the pipeline on the calling thread and bypasses the buffer.

---

## Configuration Options

### StreamPerformanceOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `EnableBufferedProcessing` | `bool` | `false` | Enables the internal buffer and consumer tasks |
| `BufferCapacity` | `int` | `10,000` | Maximum number of buffered items |
| `BackpressureStrategy` | `BackpressureStrategy` | `Block` | Behavior when the buffer is full |
| `BatchSize` | `int` | `1` | Items a consumer reads before processing them (1 = immediately) |
| `BatchTimeout` | `TimeSpan` | `100 ms` | Longest a consumer waits to fill a batch |
| `ConcurrencyLevel` | `int` | `1` | Number of parallel consumer tasks |
| `BlockingTimeout` | `TimeSpan` | `30 s` | How long `Block` waits for space before throwing |
| `OnItemDropped` | `Action<object, DropReason>` | `null` | Called for every dropped item: discarded by `DropOldest`/`DropNewest`, or rejected because the stream is stopping |
| `OnProcessingError` | `Action<object, Exception>` | `null` | Called on the consumer when an exception escapes the pipeline for a buffered item (see [Configure Error Handling](#6-configure-error-handling)) |

### Example: Custom Configuration

```csharp
var options = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BufferCapacity = 50_000,
    BackpressureStrategy = BackpressureStrategy.DropOldest,
    BatchSize = 100,
    BatchTimeout = TimeSpan.FromMilliseconds(50),
    ConcurrencyLevel = Environment.ProcessorCount,
    BlockingTimeout = TimeSpan.FromSeconds(60),
    OnItemDropped = (item, reason) => Console.WriteLine($"Dropped: {item}, Reason: {reason}")
};

var stream = StreamBuilder<SensorEvent>.CreateNewStream("CustomStream")
    .WithPerformanceOptions(options)
    .Stream()
    .Filter(e => e.Value > 0)
    .Sink(e => Console.WriteLine(e))
    .Build();
```

`SensorEvent` stands for your event type, for example `public record SensorEvent(string Id, double Value);`.

---

## Backpressure Strategies

When the buffer is full, the backpressure strategy decides what happens to a new item.

### Block (Default)

Waits for space. Best when data loss is unacceptable.

```csharp
var blocking = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BackpressureStrategy = BackpressureStrategy.Block,
    BlockingTimeout = TimeSpan.FromSeconds(30)   // Throws after this long
};
```

**Behavior:**
- `EmitAsync` waits asynchronously until there is space.
- `EmitAndForget` waits synchronously, blocking the calling thread.
- Both throw `OperationCanceledException` when `BlockingTimeout` passes. Use `Timeout.InfiniteTimeSpan` to wait forever.

### DropNewest

Rejects the **incoming** item; the items already buffered are kept. Best when the oldest waiting items must be processed.

```csharp
var dropNewest = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BackpressureStrategy = BackpressureStrategy.DropNewest
};
```

**Behavior:**
- `EmitAsync` and `EmitAndForget` never wait. `EmitAndForget` returns `false` for a rejected item; `EmitAsync` completes normally.
- The rejected item is reported to `OnItemDropped` with `DropReason.BufferFullDropNewest` and counted in `TotalDropped`.

### DropOldest

Makes room by removing the **oldest buffered** item; the new item is buffered. Best when only the latest data matters.

```csharp
var dropOldest = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BackpressureStrategy = BackpressureStrategy.DropOldest
};
```

**Behavior:**
- `EmitAsync` and `EmitAndForget` never wait and return `true` / complete normally: the new item is always buffered.
- The evicted item is reported to `OnItemDropped` with `DropReason.BufferFullDropOldest` and counted in `TotalDropped`.

### ThrowException

Throws `BufferFullException` when the buffer is full. Best when the caller must decide.

```csharp
var throwing = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BackpressureStrategy = BackpressureStrategy.ThrowException
};
```

**Behavior:**
- `EmitAndForget` and `EmitAsync` throw `BufferFullException`; the item is not buffered.
- The exception carries `BufferCapacity` and the `RejectedItem`.

```csharp
try
{
    stream.EmitAndForget(item);
}
catch (BufferFullException ex)
{
    Console.WriteLine($"Buffer full! Capacity: {ex.BufferCapacity}");
    // Retry later, or hand the item to another path
}
```

### Dropped Items

Every item the buffer does not process because of a drop is reported once to `OnItemDropped` and counted once in `BufferStatistics.TotalDropped`, with one of these reasons:

| `DropReason` | When |
|---|---|
| `BufferFullDropOldest` | `DropOldest` evicted this buffered item to make room |
| `BufferFullDropNewest` | `DropNewest` rejected this incoming item |
| `StreamStopped` | The item arrived while the stream was stopping, or was still buffered when `Stop()` ran or `StopAsync` gave up waiting (see [Graceful Shutdown](#5-graceful-shutdown)) |

`OnItemDropped` runs on the thread that emitted or stopped the stream, or on a consumer, so keep it short and thread-safe.

---

## Emit Methods

### Emit (Synchronous)

Runs the whole pipeline on the calling thread and returns when it has finished, with or without buffering.

```csharp
stream.Emit(item); // Returns after the sink has run
```

### EmitAsync

Asynchronously emits an item:
- **Without buffering**: runs the pipeline on the thread pool and completes when it has finished.
- **With buffering**: completes when the item is buffered, not when it is processed.

```csharp
await stream.EmitAsync(item);
await stream.EmitAsync(item, cancellationToken);
```

### EmitAndForget

Fire-and-forget emission. **Requires buffered processing**; throws `InvalidOperationException` otherwise.

```csharp
bool accepted = stream.EmitAndForget(item);
// Returns as soon as the item is buffered.
// accepted is false when DropNewest rejected the item or the stream is stopping.
// Under DropOldest the new item is always accepted; an older one may be evicted instead.
// Throws BufferFullException with ThrowException, and OperationCanceledException when Block times out.
```

### EmitBatchAsync

Emits several items:
- **Without buffering**: runs the pipeline once per item on the thread pool, one item after another.
- **With buffering**: buffers the items one after another, applying backpressure to each.

```csharp
var items = Enumerable.Range(1, 1000);
await stream.EmitBatchAsync(items);
await stream.EmitBatchAsync(items, cancellationToken);
```

All emit methods throw `InvalidOperationException` when the stream has not been started, or when it has a source operator.

---

## Monitoring & Statistics

### GetBufferStatistics

Returns buffer metrics, or `null` when buffered processing is disabled:

```csharp
BufferStatistics stats = stream.GetBufferStatistics();

if (stats != null)
{
    Console.WriteLine($"Current Count: {stats.CurrentCount}");
    Console.WriteLine($"Capacity: {stats.Capacity}");
    Console.WriteLine($"Utilization: {stats.UtilizationPercent:F1}%");
    Console.WriteLine($"Total Enqueued: {stats.TotalEnqueued}");
    Console.WriteLine($"Total Processed: {stats.TotalProcessed}");
    Console.WriteLine($"Total Dropped: {stats.TotalDropped}");
    Console.WriteLine($"Total Failed: {stats.TotalFailed}");
}
```

### BufferStatistics Properties

| Property | Type | Description |
|----------|------|-------------|
| `CurrentCount` | `int` | Items currently in the buffer |
| `Capacity` | `int` | Maximum buffer capacity |
| `TotalEnqueued` | `long` | Items buffered since the stream started |
| `TotalProcessed` | `long` | Items that went through the pipeline without an exception |
| `TotalDropped` | `long` | Items discarded by `DropOldest`/`DropNewest` or rejected because the stream was stopping (see [Dropped Items](#dropped-items)) |
| `TotalFailed` | `long` | Items whose processing threw an exception that error handling did not handle |
| `UtilizationPercent` | `double` | Current buffer utilization (0-100) |

### Health Monitoring Example

```csharp
public class BufferHealthMonitor : IDisposable
{
    private readonly Timer _timer;

    public BufferHealthMonitor(IStream<int> stream, Action<string> warn)
    {
        _timer = new Timer(_ =>
        {
            var stats = stream.GetBufferStatistics();
            if (stats != null && stats.UtilizationPercent > 80)
            {
                warn($"Buffer utilization high: {stats.UtilizationPercent:F1}%");
            }
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(5));
    }

    public void Dispose() => _timer.Dispose();
}
```

---

## Preset Configurations

### HighThroughput

Large buffer and parallel consumers:

```csharp
var options = StreamPerformanceOptions.HighThroughput(
    bufferCapacity: 100_000,      // Default: 100,000
    concurrencyLevel: 8);         // Default: Environment.ProcessorCount

// Equivalent to:
var equivalent = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BufferCapacity = 100_000,
    BackpressureStrategy = BackpressureStrategy.Block,
    BatchSize = 100,
    BatchTimeout = TimeSpan.FromMilliseconds(50),
    ConcurrencyLevel = 8,
    BlockingTimeout = TimeSpan.FromSeconds(60)
};
```

**Best for:** log ingestion, metrics collection, high-volume event processing where order does not matter.

### LowLatency

Immediate processing by a single consumer, so items are processed in the order they were buffered:

```csharp
var options = StreamPerformanceOptions.LowLatency(
    bufferCapacity: 10_000);      // Default: 10,000

// Equivalent to:
var equivalent = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BufferCapacity = 10_000,
    BackpressureStrategy = BackpressureStrategy.Block,
    BatchSize = 1,                // Process immediately
    ConcurrencyLevel = 1,         // Single consumer keeps the order
    BlockingTimeout = TimeSpan.FromSeconds(30)
};
```

**Best for:** real-time notifications, interactive applications.

### DropOldest

Keeps the latest data under pressure:

```csharp
var options = StreamPerformanceOptions.DropOldest(
    bufferCapacity: 10_000,
    onItemDropped: (item, reason) => Console.WriteLine($"Dropped {item}: {reason}"));

// Equivalent to:
var equivalent = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    BufferCapacity = 10_000,
    BackpressureStrategy = BackpressureStrategy.DropOldest,
    BatchSize = 1,
    ConcurrencyLevel = 1,
    OnItemDropped = (item, reason) => Console.WriteLine($"Dropped {item}: {reason}")
};
```

**Best for:** stock tickers, sensor data, real-time dashboards. The callback is called once for each item the buffer evicts.

---

## Best Practices

### 1. Choose the Right Backpressure Strategy

| Scenario | Recommended strategy |
|----------|---------------------|
| Financial transactions | `Block`: never lose data |
| Real-time metrics | `DropOldest`: keep the latest |
| Log aggregation | `Block` with a large buffer |
| Work queues where the oldest items matter most | `DropNewest` |
| Critical alerts | `ThrowException`: handle it explicitly |

### 2. Size Your Buffer

```csharp
// Rule of thumb: the buffer holds 2-5 seconds of peak throughput
var peakItemsPerSecond = 10_000;
var bufferSeconds = 3;
var bufferCapacity = peakItemsPerSecond * bufferSeconds; // 30,000
```

### 3. Use Parallel Consumers for Throughput, Not Batches

`BatchSize` only changes how many items a consumer reads from the buffer before processing them; each item still goes through the pipeline, and every sink is still called once per item. To process more items at once, raise `ConcurrencyLevel`:

```csharp
// For I/O-bound sinks (database, network)
var options = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    ConcurrencyLevel = 8,            // 8 items in flight at a time
    BatchSize = 100,                 // Fewer buffer reads under load
    BatchTimeout = TimeSpan.FromMilliseconds(100)
};
```

### 4. Monitor Buffer Health

```csharp
var stats = stream.GetBufferStatistics();

if (stats.UtilizationPercent > 90)
{
    // Add consumers or reduce the input rate
}

// After a stop, every emitted item was processed, dropped or failed
await stream.StopAsync();
var final = stream.GetBufferStatistics();
Console.WriteLine($"Dropped: {final.TotalDropped}, failed: {final.TotalFailed}");
```

### 5. Graceful Shutdown

Use `StopAsync` so buffered items are processed:

```csharp
// Bad: buffered items that were not processed yet are dropped
stream.Stop();

// Good: waits until the buffer is drained
await stream.StopAsync();
```

`StopAsync` waits until the consumers have processed every buffered item, for at most 30 seconds. Pass a cancellation token to wait less:

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
try
{
    await stream.StopAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // The buffer did not drain in time
}
```

When the token is cancelled or the 30 seconds pass, the consumers stop after the item they are processing, and the items still in the buffer are reported to `OnItemDropped` with `DropReason.StreamStopped`. A cancelled token makes `StopAsync` throw `OperationCanceledException`; the timeout does not throw. A sink that is still running is not interrupted: it finishes in the background, so give your sinks their own timeouts.

`Stop()` does not wait at all: buffered items that were not processed yet are reported as dropped with `DropReason.StreamStopped`.

### 6. Configure Error Handling

With buffered processing, nobody awaits the pipeline, so an exception that escapes it cannot reach the caller of `EmitAsync` or `EmitAndForget`. The consumer skips the item, counts it in `TotalFailed` and passes it to `OnProcessingError`, then continues with the next item:

```csharp
var options = new StreamPerformanceOptions
{
    EnableBufferedProcessing = true,
    OnProcessingError = (item, ex) => logger.LogError(ex, "Failed to process {Item}", item)
};
```

`logger` stands for your `ILogger`. To retry or skip failures per operator, configure `WithErrorHandling` as well; `OnProcessingError` only sees exceptions that error handling let through (for example with the default strategy, or after `Rethrow`).

### 7. Thread Safety with Parallel Consumers

With `ConcurrencyLevel > 1`, operators and sinks run on several threads at once:

```csharp
// Bad: List<T> is not thread-safe
var list = new List<int>();
builder.Sink(x => list.Add(x));

// Good: a thread-safe collection
var bag = new ConcurrentBag<int>();
builder.Sink(x => bag.Add(x));

// Good: atomic operations
var counter = 0;
builder.Sink(x => Interlocked.Increment(ref counter));
```

---

## Migration Guide

### From Synchronous to Async (Minimal Change)

Existing code continues to work unchanged:

```csharp
// Before (still works)
stream.Emit(item);

// After (same processing, awaited on the thread pool)
await stream.EmitAsync(item);
```

### Enabling Buffered Processing

Add performance options without changing the pipeline:

```csharp
// Before
var stream = StreamBuilder<int>.CreateNewStream("MyStream")
    .Stream()
    .Map(x => x * 2)
    .Sink(x => Console.WriteLine(x))
    .Build();

// After (just add WithPerformanceOptions)
var buffered = StreamBuilder<int>.CreateNewStream("MyStream")
    .WithPerformanceOptions(StreamPerformanceOptions.LowLatency())
    .Stream()
    .Map(x => x * 2)
    .Sink(x => Console.WriteLine(x))
    .Build();
```

### Updating Stop Calls

Replace the synchronous stop with the async one for a graceful shutdown:

```csharp
// Before
stream.Stop();

// After
await stream.StopAsync();
```

---

## API Reference

### IStream&lt;TIn&gt; Methods

| Method | Description |
|--------|-------------|
| `void Emit(TIn value)` | Synchronous emission (returns after processing) |
| `Task EmitAsync(TIn value, CancellationToken ct)` | Async emission |
| `Task EmitBatchAsync(IEnumerable<TIn> values, CancellationToken ct)` | Emits several items |
| `bool EmitAndForget(TIn value)` | Fire-and-forget (requires buffering) |
| `void Start()` | Starts the stream |
| `void Stop()` | Stops immediately |
| `Task StopAsync(CancellationToken ct)` | Graceful async stop |
| `BufferStatistics GetBufferStatistics()` | Buffer metrics (`null` without buffering) |

### IInitialStreamBuilder&lt;TIn&gt; Methods

| Method | Description |
|--------|-------------|
| `WithPerformanceOptions(StreamPerformanceOptions)` | Configures performance options |
| `WithErrorHandling(StreamExecutionOptions)` | Configures error handling |
| `WithTelemetry(ITelemetryProvider)` | Configures telemetry |

### Exceptions

| Exception | When thrown |
|-----------|-------------|
| `BufferFullException` | The buffer is full with the `ThrowException` strategy |
| `OperationCanceledException` | `Block` timed out, or the cancellation token was cancelled |
| `InvalidOperationException` | `EmitAndForget` without buffering; any emit before `Start()` or on a stream with a source operator |

---

## Complete Example

```csharp
public record Order(string Id, decimal Amount, bool IsValid);
public record ProcessedOrder(string Id, decimal Total);

public interface IOrderServices
{
    Order Enrich(Order order);
    ProcessedOrder Process(Order order);
    void Save(ProcessedOrder order);
}

public interface IAppLog
{
    void Info(string message);
    void Warn(string message);
}

public class OrderProcessor
{
    private readonly IStream<Order> _stream;
    private readonly IAppLog _log;

    public OrderProcessor(IOrderServices services, IAppLog log)
    {
        _log = log;
        _stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
            .WithPerformanceOptions(new StreamPerformanceOptions
            {
                EnableBufferedProcessing = true,
                BufferCapacity = 50_000,
                BackpressureStrategy = BackpressureStrategy.Block,
                ConcurrencyLevel = 4
            })
            .WithErrorHandling(new StreamExecutionOptions
            {
                ErrorHandlingStrategy = ErrorHandlingStrategy.Retry,
                MaxRetries = 3,
                RetryDelay = TimeSpan.FromSeconds(1)
            })
            .Stream()
            .Filter(order => order.IsValid)
            .Map(order => services.Enrich(order))
            .Map(order => services.Process(order))
            .Sink(order => services.Save(order))
            .Build();
    }

    public void Start() => _stream.Start();

    public Task StopAsync() => _stream.StopAsync();

    public Task SubmitOrderAsync(Order order) => _stream.EmitAsync(order);

    public void SubmitOrderFireAndForget(Order order)
    {
        if (!_stream.EmitAndForget(order))
        {
            _log.Warn($"Order {order.Id} was rejected: the stream is stopping");
        }
    }

    public void LogStats()
    {
        var stats = _stream.GetBufferStatistics();
        if (stats != null)
        {
            _log.Info($"Buffer: {stats.CurrentCount}/{stats.Capacity} " +
                      $"({stats.UtilizationPercent:F1}%), " +
                      $"Processed: {stats.TotalProcessed}, " +
                      $"Dropped: {stats.TotalDropped}");
        }
    }
}
```

With `Retry`, an operator that keeps failing after `MaxRetries` attempts stops the stream; use `Skip`, or an `OnError` callback, if one bad order must not stop processing.
