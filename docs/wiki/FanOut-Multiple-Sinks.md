# FanOut - Multiple Sinks Pattern

**FanOut** sends every item of a stream to several destinations (sinks). Use it for dual writes, multi-channel notifications, analytics feeds and audit logging.

## Table of Contents

- [Overview](#overview)
- [When to Use FanOut](#when-to-use-fanout)
- [FanOut vs AddBranch](#fanout-vs-addbranch)
- [Basic Usage](#basic-usage)
- [API Reference](#api-reference)
- [Real-World Examples](#real-world-examples)
  - [E-Commerce Order Processing](#e-commerce-order-processing)
  - [IoT Sensor Data Processing](#iot-sensor-data-processing)
  - [Log Aggregation Pipeline](#log-aggregation-pipeline)
  - [Financial Transaction Processing](#financial-transaction-processing)
  - [User Activity Tracking](#user-activity-tracking)
- [Best Practices](#best-practices)
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)

---

## Overview

A FanOut hands each item to every sink you configure. A sink can have a filter predicate so it only receives matching items, or a transformation so it receives a different type.

```
                         +--> database
                         |
source --> [operators] --+--> kafka
                         |
                         +--> alerts (only amount > 10,000)
```

## When to Use FanOut

Use FanOut when you need to:

- **Dual-write** to several storage systems (database and cache)
- **Publish events** to several message brokers
- **Send notifications** over several channels (email, SMS, push)
- **Feed several analytics** systems from the same data
- **Keep an audit trail** while processing data
- **Route data conditionally** to different sinks

## FanOut vs AddBranch

| Feature | `FanOut` | `AddBranch` |
|---------|----------|-------------|
| **Purpose** | Several sinks, same or filtered data | Branches with their own processing |
| **API style** | Fluent, focused on sinks | A full branch pipeline per branch |
| **Transformations** | One per sink, via `ToWithTransform` | Any operators per branch |
| **After it** | Nothing; `FanOut` ends the pipeline with `Build()` | The main pipeline continues |
| **Complexity** | Simple | More flexible |

**Rule of thumb:** use `FanOut` for several sinks with optional filtering. Use `AddBranch` when each branch needs its own processing, and `Merge` when branch results should continue as one stream. See [Branches, Merge and Dynamic Branching](Branch-and-Merge.md).

## Basic Usage

The snippets in this section use this model and these placeholder methods:

```csharp
public record Order(string Id, decimal Amount, bool IsPriority);
public record OrderEvent(string OrderId, string Type, DateTime OccurredAt);
public record OrderMetrics(string OrderId, decimal Amount);

static void SaveToDatabase(Order order) { /* write to your database */ }
static void PublishToKafka(Order order) { /* publish to your broker */ }
static void LogOrder(Order order) { /* write a log line */ }
static void SendAlert(Order order) { /* notify someone */ }
static void EnqueuePriority(Order order) { /* fast-track queue */ }
static void PublishEvent(OrderEvent orderEvent) { /* publish an event */ }
static void RecordMetrics(OrderMetrics metrics) { /* analytics */ }
```

### Simple Multi-Sink

```csharp
var stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
    .Stream()
    .FanOut(fanOut => fanOut
        .To("database", order => SaveToDatabase(order))
        .To("kafka", order => PublishToKafka(order))
        .To("logging", order => LogOrder(order)))
    .Build();

stream.Start();
stream.Emit(new Order("ORD-001", 100m, IsPriority: false));
```

### With Filtering

```csharp
var stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
    .Stream()
    .FanOut(fanOut => fanOut
        // All orders to the database
        .To("database", order => SaveToDatabase(order))
        // Only high-value orders to alerts
        .To("alerts",
            order => order.Amount > 10_000m,
            order => SendAlert(order))
        // Only priority orders to the fast-track queue
        .To("priority-queue",
            order => order.IsPriority,
            order => EnqueuePriority(order)))
    .Build();
```

### With Transformation

```csharp
var stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
    .Stream()
    .FanOut(fanOut => fanOut
        // Store the original order
        .To("database", order => SaveToDatabase(order))
        // Transform to an event for Kafka
        .ToWithTransform("kafka",
            order => new OrderEvent(order.Id, "Created", DateTime.UtcNow),
            orderEvent => PublishEvent(orderEvent))
        // Transform to metrics for analytics
        .ToWithTransform("analytics",
            order => new OrderMetrics(order.Id, order.Amount),
            metrics => RecordMetrics(metrics)))
    .Build();
```

## API Reference

### `IFanOutBuilder<TIn, TCurrent>`

| Method | Description |
|--------|-------------|
| `To(string name, Action<TCurrent> sinkFunction)` | Adds a named sink that receives every item |
| `To(string name, Func<TCurrent, bool> predicate, Action<TCurrent> sinkFunction)` | Adds a sink that receives only matching items |
| `To(string name, ISinkOperator<TCurrent> sinkOperator)` | Adds a custom sink operator |
| `To(string name, Func<TCurrent, bool> predicate, ISinkOperator<TCurrent> sinkOperator)` | Adds a custom sink operator for matching items |
| `ToWithTransform<TOutput>(string name, Func<TCurrent, TOutput> mapFunction, Action<TOutput> sinkFunction)` | Adds a sink that receives a transformed item |
| `Build()` | Builds the stream |

### Important Notes

- **Sink names must be unique** within a FanOut, ignoring case. A duplicate name throws `ArgumentException`.
- **At least one sink** must be configured; `Build()` throws `InvalidOperationException` otherwise.
- **Sinks run one after another**, in the order they were added, on the thread that emitted the item. They do not run in parallel.
- **A failing sink stops the item** unless you configure error handling: without it, the exception leaves `Emit` and the sinks after it do not receive that item. With `ErrorHandlingStrategy.Skip`, only the failing sink skips the item. See [Error Handling](#error-handling).
- `GetBranches()` lists the sinks by name, and a running stream can get extra sinks with `stream.AddBranch<T>(...)`, which attaches to the FanOut. See [Dynamic Branching](Branch-and-Merge.md#dynamic-branching).

---

## Real-World Examples

Each example declares the records and service interfaces it uses, so you can paste it into a project that references `Cortex.Streams` and plug in your own implementations.

### E-Commerce Order Processing

Confirmed orders go to the database, to Kafka as an event, to an alert service when they are large, and to analytics as metrics.

```csharp
public enum OrderStatus { Pending, Confirmed, Cancelled }
public record OrderItem(string Sku, int Quantity);
public record ShopOrder(string Id, string CustomerId, OrderStatus Status, decimal TotalAmount, string Region, IReadOnlyList<OrderItem> Items);
public record OrderConfirmedEvent(string OrderId, string CustomerId, decimal Amount, DateTime Timestamp);
public record ShopOrderMetrics(string OrderId, decimal Amount, int ItemCount, string Region);

public interface IOrderRepository { void Save(ShopOrder order); }
public interface IEventPublisher { void Publish(string topic, OrderConfirmedEvent orderEvent); }
public interface IAlertService { void SendHighValueOrderAlert(ShopOrder order); }
public interface IAnalyticsService { void RecordOrderMetrics(ShopOrderMetrics metrics); }

public class OrderProcessor
{
    private readonly IOrderRepository _repository;
    private readonly IEventPublisher _events;
    private readonly IAlertService _alerts;
    private readonly IAnalyticsService _analytics;

    public OrderProcessor(IOrderRepository repository, IEventPublisher events, IAlertService alerts, IAnalyticsService analytics)
    {
        _repository = repository;
        _events = events;
        _alerts = alerts;
        _analytics = analytics;
    }

    public IStream<ShopOrder> SetupPipeline()
    {
        var stream = StreamBuilder<ShopOrder>.CreateNewStream("OrderProcessingPipeline")
            .Stream()
            // Only process confirmed orders
            .Filter(order => order.Status == OrderStatus.Confirmed)
            .FanOut(fanOut => fanOut
                // 1. Persist to the database (primary storage)
                .To("database", order => _repository.Save(order))

                // 2. Publish an event for downstream consumers
                .ToWithTransform("order-events",
                    order => new OrderConfirmedEvent(order.Id, order.CustomerId, order.TotalAmount, DateTime.UtcNow),
                    orderEvent => _events.Publish("order-events", orderEvent))

                // 3. Alert on high-value orders (over 10,000)
                .To("high-value-alerts",
                    order => order.TotalAmount > 10_000m,
                    order => _alerts.SendHighValueOrderAlert(order))

                // 4. Send metrics to analytics
                .ToWithTransform("analytics",
                    order => new ShopOrderMetrics(order.Id, order.TotalAmount, order.Items.Count, order.Region),
                    metrics => _analytics.RecordOrderMetrics(metrics)))
            .Build();

        stream.Start();
        return stream;
    }
}
```

### IoT Sensor Data Processing

Route sensor readings to storage tiers by criticality.

```csharp
public record SensorReading(string SensorId, double Temperature, DateTime Timestamp);
public record DashboardUpdate(string SensorId, double Value, string Status, DateTime Timestamp);

public interface IColdStorage { void Archive(SensorReading reading); }
public interface IHotStorage { void Store(SensorReading reading); }
public interface IPagerService { void TriggerAlert(string message); }
public interface IWarningLog { void Log(SensorReading reading); }
public interface ITimeSeriesDb { void Insert(SensorReading reading); }
public interface IDashboard { void Push(DashboardUpdate update); }

public class SensorDataPipeline
{
    private readonly IColdStorage _coldStorage;
    private readonly IHotStorage _hotStorage;
    private readonly IPagerService _pager;
    private readonly IWarningLog _warningLog;
    private readonly ITimeSeriesDb _timeSeriesDb;
    private readonly IDashboard _dashboard;

    public SensorDataPipeline(IColdStorage coldStorage, IHotStorage hotStorage, IPagerService pager,
        IWarningLog warningLog, ITimeSeriesDb timeSeriesDb, IDashboard dashboard)
    {
        _coldStorage = coldStorage;
        _hotStorage = hotStorage;
        _pager = pager;
        _warningLog = warningLog;
        _timeSeriesDb = timeSeriesDb;
        _dashboard = dashboard;
    }

    public IStream<SensorReading> Create()
    {
        return StreamBuilder<SensorReading>.CreateNewStream("SensorDataPipeline")
            .Stream()
            .FanOut(fanOut => fanOut
                // Archive ALL readings to cold storage
                .To("cold-storage", reading => _coldStorage.Archive(reading))

                // Critical readings (over 100 degrees C): page someone and keep in hot storage
                .To("critical-alerts",
                    reading => reading.Temperature > 100,
                    reading =>
                    {
                        _pager.TriggerAlert($"CRITICAL: {reading.SensorId} at {reading.Temperature} C");
                        _hotStorage.Store(reading);
                    })

                // Warning readings (80 to 100 degrees C): log for review
                .To("warning-log",
                    reading => reading.Temperature >= 80 && reading.Temperature <= 100,
                    reading => _warningLog.Log(reading))

                // Normal readings to the time-series database
                .To("timeseries-db",
                    reading => reading.Temperature < 80,
                    reading => _timeSeriesDb.Insert(reading))

                // All readings to the real-time dashboard
                .ToWithTransform("dashboard",
                    reading => new DashboardUpdate(reading.SensorId, reading.Temperature, GetStatus(reading.Temperature), reading.Timestamp),
                    update => _dashboard.Push(update)))
            .Build();
    }

    private static string GetStatus(double temperature) => temperature switch
    {
        > 100 => "CRITICAL",
        >= 80 => "WARNING",
        _ => "NORMAL"
    };
}
```

### Log Aggregation Pipeline

Enrich logs from several services and route them by severity.

```csharp
public enum Severity { Debug, Information, Warning, Error }
public record LogEntry(string Id, string Service, Severity Level, string Message, string StackTrace, DateTime Timestamp,
    DateTime? ProcessedAt = null, string Environment = null);
public record Incident(string Title, string Severity, string Details);

public interface ISearchIndex { void Index(string index, LogEntry entry); }
public interface IIncidentService { void CreateIncident(Incident incident); }
public interface IChatNotifier { void PostMessage(string channel, string text); }
public interface IMetricsCounter { void Increment(string name, string service, string level); }
public interface IArchive { void Put(string key, string content); }

public class LogAggregationPipeline
{
    private readonly ISearchIndex _search;
    private readonly IIncidentService _incidents;
    private readonly IChatNotifier _chat;
    private readonly IMetricsCounter _metrics;
    private readonly IArchive _archive;
    private readonly string _environment;

    public LogAggregationPipeline(ISearchIndex search, IIncidentService incidents, IChatNotifier chat,
        IMetricsCounter metrics, IArchive archive, string environment)
    {
        _search = search;
        _incidents = incidents;
        _chat = chat;
        _metrics = metrics;
        _archive = archive;
        _environment = environment;
    }

    public IStream<LogEntry> Create()
    {
        return StreamBuilder<LogEntry>.CreateNewStream("LogAggregation")
            .Stream()
            // Enrich logs with metadata
            .Map(log => log with { ProcessedAt = DateTime.UtcNow, Environment = _environment })
            .FanOut(fanOut => fanOut
                // All logs to the search index
                .To("search", log => _search.Index("logs", log))

                // Errors become incidents for the on-call engineer
                .To("incidents",
                    log => log.Level == Severity.Error,
                    log => _incidents.CreateIncident(new Incident($"[{log.Service}] {log.Message}", "high", log.StackTrace)))

                // Warnings go to a chat channel
                .To("chat",
                    log => log.Level == Severity.Warning,
                    log => _chat.PostMessage("#alerts", $"[{log.Service}] {log.Message}"))

                // A counter per service and level
                .To("metrics", log => _metrics.Increment("log_entries_total", log.Service, log.Level.ToString()))

                // Long-term archive, one JSON document per entry
                .To("archive",
                    log => _archive.Put($"logs/{log.Timestamp:yyyy/MM/dd}/{log.Id}.json", JsonSerializer.Serialize(log))))
            .Build();
    }
}
```

`JsonSerializer` is `System.Text.Json.JsonSerializer`.

### Financial Transaction Processing

Every transaction is recorded; the compliance sinks receive only the transactions they are responsible for.

```csharp
public enum TransactionType { Domestic, International }
public record Transaction(string Id, string AccountId, string AccountHolder, decimal Amount, TransactionType Type);
public record CurrencyTransactionReport(string TransactionId, decimal Amount, string AccountHolder, DateTime FilingDate);
public record AuditEntry(string EntityType, string EntityId, string Action, DateTime Timestamp, string Details);

public interface ILedger { void RecordTransaction(Transaction transaction); }
public interface IFraudService { void Analyze(Transaction transaction); }
public interface IComplianceService { void FileCtr(CurrencyTransactionReport report); }
public interface ISanctionsScreening { void Screen(Transaction transaction); }
public interface IAuditLog { void Append(AuditEntry entry); }
public interface IBalanceService { void UpdateBalance(string accountId, decimal amount); }

public class TransactionPipeline
{
    private readonly ILedger _ledger;
    private readonly IFraudService _fraud;
    private readonly IComplianceService _compliance;
    private readonly ISanctionsScreening _sanctions;
    private readonly IAuditLog _auditLog;
    private readonly IBalanceService _balances;

    public TransactionPipeline(ILedger ledger, IFraudService fraud, IComplianceService compliance,
        ISanctionsScreening sanctions, IAuditLog auditLog, IBalanceService balances)
    {
        _ledger = ledger;
        _fraud = fraud;
        _compliance = compliance;
        _sanctions = sanctions;
        _auditLog = auditLog;
        _balances = balances;
    }

    public IStream<Transaction> Create()
    {
        return StreamBuilder<Transaction>.CreateNewStream("TransactionProcessing")
            .Stream()
            .FanOut(fanOut => fanOut
                // Main ledger: every transaction
                .To("ledger", txn => _ledger.RecordTransaction(txn))

                // Fraud detection: large international transactions
                .To("fraud-detection",
                    txn => txn.Amount > 10_000m && txn.Type == TransactionType.International,
                    txn => _fraud.Analyze(txn))

                // Regulatory reporting: a currency transaction report above 10,000
                .To("regulatory-ctr",
                    txn => txn.Amount > 10_000m,
                    txn => _compliance.FileCtr(new CurrencyTransactionReport(txn.Id, txn.Amount, txn.AccountHolder, DateTime.UtcNow)))

                // Sanctions screening: international transactions
                .To("sanctions-screening",
                    txn => txn.Type == TransactionType.International,
                    txn => _sanctions.Screen(txn))

                // Audit trail: an immutable record of every transaction
                .ToWithTransform("audit-log",
                    txn => new AuditEntry("Transaction", txn.Id, "PROCESSED", DateTime.UtcNow, JsonSerializer.Serialize(txn)),
                    entry => _auditLog.Append(entry))

                // Real-time balance update
                .To("balance-service", txn => _balances.UpdateBalance(txn.AccountId, txn.Amount)))
            .Build();
    }
}
```

### User Activity Tracking

Send user activity to several analytics systems, each receiving only the activity types it needs.

```csharp
public enum ActivityType { PageView, Purchase, Search }
public record UserActivity(string UserId, string SessionId, ActivityType Type, string PageUrl, string Details, DateTime Timestamp);
public record PurchaseEvent(string UserId, string ProductId, DateTime Timestamp);
public record SessionMetric(string SessionId, string UserId, int ActivityCount, DateTime LastActivity);

public interface IDataLake { void Store(string dataset, UserActivity activity); }
public interface IWebAnalytics { void TrackPageView(string userId, string pageUrl); }
public interface ICommerceAnalytics { void TrackPurchase(PurchaseEvent purchase); }
public interface ISearchAnalytics { void RecordQuery(string query); }
public interface ISessionTracker { void UpdateMetrics(SessionMetric metric); }
public interface IPersonalization { void UpdateUserProfile(UserActivity activity); }

public class UserActivityPipeline
{
    private readonly IDataLake _dataLake;
    private readonly IWebAnalytics _webAnalytics;
    private readonly ICommerceAnalytics _commerce;
    private readonly ISearchAnalytics _searchAnalytics;
    private readonly ISessionTracker _sessions;
    private readonly IPersonalization _personalization;

    public UserActivityPipeline(IDataLake dataLake, IWebAnalytics webAnalytics, ICommerceAnalytics commerce,
        ISearchAnalytics searchAnalytics, ISessionTracker sessions, IPersonalization personalization)
    {
        _dataLake = dataLake;
        _webAnalytics = webAnalytics;
        _commerce = commerce;
        _searchAnalytics = searchAnalytics;
        _sessions = sessions;
        _personalization = personalization;
    }

    public IStream<UserActivity> Create()
    {
        return StreamBuilder<UserActivity>.CreateNewStream("UserActivityTracking")
            .Stream()
            .FanOut(fanOut => fanOut
                // Raw clickstream to the data lake
                .To("data-lake", activity => _dataLake.Store("clickstream", activity))

                // Page views to web analytics
                .To("web-analytics",
                    activity => activity.Type == ActivityType.PageView,
                    activity => _webAnalytics.TrackPageView(activity.UserId, activity.PageUrl))

                // Purchases to commerce analytics
                .To("commerce-analytics",
                    activity => activity.Type == ActivityType.Purchase,
                    activity => _commerce.TrackPurchase(new PurchaseEvent(activity.UserId, activity.Details, activity.Timestamp)))

                // Search queries for search optimisation
                .To("search-analytics",
                    activity => activity.Type == ActivityType.Search,
                    activity => _searchAnalytics.RecordQuery(activity.Details))

                // Session metrics for engagement
                .ToWithTransform("session-metrics",
                    activity => new SessionMetric(activity.SessionId, activity.UserId, 1, activity.Timestamp),
                    metric => _sessions.UpdateMetrics(metric))

                // Real-time personalisation
                .To("personalization", activity => _personalization.UpdateUserProfile(activity)))
            .Build();
    }
}
```

---

## Best Practices

### 1. Use Meaningful Sink Names

```csharp
// Good: descriptive names
.To("order-database", ...)
.To("kafka-order-events", ...)
.To("high-value-alerts", ...)

// Avoid: generic names
.To("sink1", ...)
.To("output", ...)
```

Sink names appear in `GetBranches()`, in telemetry metric names and in `RemoveBranch(name)`.

### 2. Handle Errors Per Sink When One Sink Must Not Affect Others

Stream-level error handling (see [Error Handling](#error-handling)) applies to every sink. When a single sink needs its own fallback, handle it inside the sink:

```csharp
.To("database", order =>
{
    try
    {
        repository.Save(order);
    }
    catch (Exception ex)
    {
        logger.Error(ex, $"Failed to save order {order.Id}");
        deadLetterQueue.Enqueue(order);
    }
})
```

### 3. Keep Blocking Calls Short

Sinks run on the thread that emitted the item, one after another, so a slow sink delays every sink after it and the caller of `Emit`. Prefer handing work to a queue or outbox over long synchronous calls:

```csharp
// Hands the order to a background worker instead of calling the remote API here
.To("crm", order => crmOutbox.Enqueue(order))
```

If a sink must call an async API, it has to block on it (`.GetAwaiter().GetResult()`), because sinks are synchronous.

### 4. Apply Filters at the FanOut Level

```csharp
// Good: the predicate decides which items reach the sink
.FanOut(fanOut => fanOut
    .To("alerts", order => order.Amount > 10_000m, order => SendAlert(order)))

// Avoid: filtering inside the sink
.FanOut(fanOut => fanOut
    .To("alerts", order =>
    {
        if (order.Amount > 10_000m)
            SendAlert(order);
    }))
```

A predicate is also counted separately in telemetry and follows the stream's error handling.

### 5. Keep Transformations Simple

```csharp
// Good: a simple projection
.ToWithTransform("events",
    order => new OrderEvent(order.Id, "Created", DateTime.UtcNow),
    orderEvent => PublishEvent(orderEvent))

// Avoid: complex logic in the transform; put it in a named method or an AddBranch with its own operators
```

---

## Error Handling

Configure error handling for the whole stream with `WithErrorHandling`. It applies to every sink, predicate and transformation of the FanOut.

```csharp
var stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
    .WithErrorHandling(new StreamExecutionOptions
    {
        // Used when OnError is not set: None (rethrow), Skip, Retry or Stop
        ErrorHandlingStrategy = ErrorHandlingStrategy.Skip,
        MaxRetries = 3,
        RetryDelay = TimeSpan.FromSeconds(1),
        // Optional: decide per error; overrides ErrorHandlingStrategy
        OnError = context =>
        {
            Console.WriteLine($"{context.OperatorName} failed on attempt {context.Attempt}: {context.Exception.Message}");
            return context.Attempt < 3 ? ErrorHandlingDecision.Retry : ErrorHandlingDecision.Skip;
        }
    })
    .Stream()
    .FanOut(fanOut => fanOut
        .To("database", order => SaveToDatabase(order))
        .To("kafka", order => PublishToKafka(order)))
    .Build();
```

| Decision | Effect |
|----------|--------|
| `Skip` | The failing sink skips the item; the other sinks still receive it. |
| `Retry` | The sink is called again after `RetryDelay`. When the attempts reach `MaxRetries`, the stream stops. |
| `Stop` | The stream stops; `Emit` returns and `GetStatus()` reports `NOT_RUNNING`. |
| `Rethrow` | The exception leaves `Emit`; the sinks after the failing one do not receive the item. |

---

## Performance Considerations

1. **Sink execution**: sinks run one after another within the FanOut, on the emitting thread. For high throughput, enable buffered processing (see [Stream Performance](stream-performance.md)) or hand slow work to a queue.

2. **Filter early**: put `Filter` before `FanOut`, or predicates on the sinks, so expensive sinks receive less data.

3. **Monitor sink latency**: with telemetry enabled, every sink reports its own processing-time histogram (`branch_operator_processing_time_<sink name>_<type>`).

4. **Buffer bursts**: for bursty workloads, enable buffered processing with a backpressure strategy instead of slowing producers down.

```csharp
// Per-sink metrics through telemetry
var stream = StreamBuilder<Order>.CreateNewStream("OrderProcessor")
    .WithTelemetry(telemetryProvider)
    .Stream()
    .FanOut(fanOut => fanOut
        .To("fast-sink", order => LogOrder(order))
        .To("slow-sink", order => SaveToDatabase(order)))
    .Build();
```

---

## Related Documentation

- [Branches, Merge and Dynamic Branching](Branch-and-Merge.md)
- [Stream Performance and Async Processing](stream-performance.md)
- [Stream-Table Joins](Stream-Table-Joins.md)
- [Stream-Stream Windowed Joins](Stream-Stream-Joins.md)
