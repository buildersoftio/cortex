# Stream-Stream Windowed Joins in Cortex.Streams

This guide covers how to use **Stream-Stream Joins** in Cortex.Streams to correlate events from two unbounded streams within a time window.

## Overview

A stream-stream join matches events from two streams that share a key and arrive within a time window of each other. Use it for:
- Correlating events that happen close together in time
- Matching requests with responses
- Joining data from different sources that share an identifier

### When to Use Stream-Stream Joins

| Scenario | Example |
|----------|---------|
| **Event correlation** | Match orders with shipments |
| **Request-response matching** | Pair ride requests with driver assignments |
| **Cross-system integration** | Join ad impressions with clicks |
| **Fraud detection** | Correlate login events with transaction events |
| **IoT data fusion** | Combine readings from several sensors |

For enriching a stream with reference data that changes slowly (customers, products), use a [Stream-Table Join](Stream-Table-Joins.md) instead.

---

## How Matching Works

`StreamStreamJoinOperator<TLeft, TRight, TKey, TResult>` keeps two buffers:

- The **left side** is the stream the operator is part of: every item that reaches `.JoinStream(joinOperator)` is a left event.
- The **right side** is fed by calling `joinOperator.ProcessRight(item)`, for example from a second stream, a message consumer or a webhook.

When an event arrives on one side, it is buffered and matched against every **unexpired** event with the same key on the other side:

- An event expires at `timestamp + WindowSize`, compared with the current UTC time. Timestamps must therefore be **UTC** and close to the current time; an event whose timestamp is already older than the window never matches.
- **Every** matching pair produces a result. An order with two buffered executions produces two results.
- A result for a match is passed downstream on the thread that delivered the second event: `Emit` for left events, `ProcessRight` for right events.
- Matched events stay buffered until they expire, so later events can still match them.

Unmatched events are handled by a cleanup timer that runs every `CleanupInterval` (default 30 seconds). An event is removed once it is older than `WindowSize + GracePeriod`; for left, right and outer joins an unmatched one is then emitted with `default` (`null` for classes and records) for the missing side. These results are passed downstream **on the timer thread**.

## Join Types

| Join type | Unmatched left event | Unmatched right event | How to configure |
|-----------|----------------------|-----------------------|------------------|
| **Inner** | Dropped | Dropped | `StreamJoinConfiguration.InnerJoin(window)` (the default) |
| **Left** | Emitted with `default` right | Dropped | `StreamJoinConfiguration.LeftJoin(window)` |
| **Right** | Dropped | Emitted with `default` left | `new StreamJoinConfiguration { JoinType = StreamJoinType.Right }` |
| **Outer** | Emitted with `default` right | Emitted with `default` left | `StreamJoinConfiguration.OuterJoin(window)` |

For left, right and outer joins the join function is called with `null` for the missing side, so it must not dereference that side without a null check.

---

## Basic Usage

The examples on this page use these namespaces:

```csharp
using Cortex.Streams;
using Cortex.Streams.Operators;          // StreamStreamJoinOperator
using Cortex.Streams.Operators.Joins;    // StreamJoinConfiguration, StreamJoinType
```

### Example: Matching Orders with Shipments

```csharp
// Define models
public record Order(string OrderId, int CustomerId, decimal Amount, DateTime Timestamp);
public record Shipment(string ShipmentId, string OrderId, string Carrier, DateTime ShippedAt);
public record OrderShipment(Order Order, Shipment Shipment);
```

```csharp
// Create the join operator
var joinOperator = new StreamStreamJoinOperator<Order, Shipment, string, OrderShipment>(
    // Key selectors
    order => order.OrderId,
    shipment => shipment.OrderId,
    // Timestamp selectors (UTC)
    order => order.Timestamp,
    shipment => shipment.ShippedAt,
    // Join function
    (order, shipment) => new OrderShipment(order, shipment),
    // Configuration: 1-hour window, inner join
    StreamJoinConfiguration.InnerJoin(TimeSpan.FromHours(1)));

// Build the order stream (left side)
var orderStream = StreamBuilder<Order>.CreateNewStream("OrderShipmentJoin")
    .Stream()
    .JoinStream(joinOperator)
    .Sink(result => Console.WriteLine($"Order {result.Order.OrderId} shipped via {result.Shipment.Carrier}!"))
    .Build();

// Start the stream
orderStream.Start();

// Emit orders to the left side
orderStream.Emit(new Order("ORD-001", 100, 150.00m, DateTime.UtcNow));
orderStream.Emit(new Order("ORD-002", 101, 75.50m, DateTime.UtcNow));

// Feed shipments to the right side (from a message queue, a webhook, ...)
joinOperator.ProcessRight(new Shipment("SHP-001", "ORD-001", "FedEx", DateTime.UtcNow));  // Matches ORD-001
joinOperator.ProcessRight(new Shipment("SHP-002", "ORD-002", "UPS", DateTime.UtcNow));    // Matches ORD-002
```

**Output:**
```
Order ORD-001 shipped via FedEx!
Order ORD-002 shipped via UPS!
```

The two lines are written by the `ProcessRight` calls, because each shipment is the second event of its pair. An inner join drops orders that are never shipped within the hour.

---

## Real-World Use Cases

Each use case declares the records and service interfaces it uses, so you can paste it into a project that references `Cortex.Streams` and plug in your own implementations.

### 1. E-Commerce: Order Payment Tracking

Paid orders start shipping as soon as the payment arrives. Orders without a payment within 30 minutes (plus a 5-minute grace period for late payments) trigger a reminder.

```csharp
public record OrderPlaced(string OrderId, int CustomerId, List<string> Items, decimal Total, DateTime Timestamp);
public record PaymentReceived(string PaymentId, string OrderId, decimal Amount, string Method, DateTime Timestamp);
public record OrderFulfillment(string OrderId, int CustomerId, decimal Total, bool IsPaid, string PaymentMethod, TimeSpan? PaymentDelay);

public interface IFulfillmentActions
{
    void StartShipment(OrderFulfillment fulfillment);
    void SendPaymentReminder(int customerId, string orderId);
}

public class OrderPaymentTracking
{
    private readonly StreamStreamJoinOperator<OrderPlaced, PaymentReceived, string, OrderFulfillment> _join;
    private readonly IStream<OrderPlaced> _orders;

    public OrderPaymentTracking(IFulfillmentActions actions)
    {
        // Payments should arrive within 30 minutes of the order
        _join = new StreamStreamJoinOperator<OrderPlaced, PaymentReceived, string, OrderFulfillment>(
            order => order.OrderId,
            payment => payment.OrderId,
            order => order.Timestamp,
            payment => payment.Timestamp,
            (order, payment) => new OrderFulfillment(
                order.OrderId,
                order.CustomerId,
                order.Total,
                payment != null,
                payment?.Method,
                payment != null ? payment.Timestamp - order.Timestamp : (TimeSpan?)null),
            new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(30),
                JoinType = StreamJoinType.Left,         // Emit orders without payment too, for follow-up
                GracePeriod = TimeSpan.FromMinutes(5)   // Accept slightly late payments
            });

        _orders = StreamBuilder<OrderPlaced>.CreateNewStream("OrderFulfillment")
            .Stream()
            .JoinStream(_join)
            .Sink(fulfillment =>
            {
                if (fulfillment.IsPaid)
                    actions.StartShipment(fulfillment);
                else
                    actions.SendPaymentReminder(fulfillment.CustomerId, fulfillment.OrderId);
            })
            .Build();
        _orders.Start();
    }

    public void OnOrderPlaced(OrderPlaced order) => _orders.Emit(order);

    public void OnPaymentReceived(PaymentReceived payment) => _join.ProcessRight(payment);
}
```

A paid order reaches `StartShipment` from `OnPaymentReceived`. An unpaid order reaches `SendPaymentReminder` from the cleanup timer, between 35 and 35.5 minutes after it was placed (window, grace period and the 30-second cleanup interval).

---

### 2. Ride-Sharing: Matching Ride Requests with Driver Assignments

```csharp
public record Location(double Latitude, double Longitude);
public record RideRequest(string RequestId, string UserId, Location Pickup, Location Dropoff, DateTime Timestamp);
public record DriverAssignment(string AssignmentId, string RequestId, string DriverId, string VehicleInfo, DateTime Timestamp);
public record RideMatch(string RequestId, string UserId, Location Pickup, string DriverId, string VehicleInfo, bool IsMatched, TimeSpan? WaitTime);

public interface IRiderNotifications
{
    void DriverAssigned(string userId, string driverId, string vehicleInfo);
    void OfferAlternatives(string userId, Location pickup);
}

public class RideMatching
{
    private readonly StreamStreamJoinOperator<RideRequest, DriverAssignment, string, RideMatch> _join;
    private readonly IStream<RideRequest> _requests;

    public RideMatching(IRiderNotifications riders)
    {
        _join = new StreamStreamJoinOperator<RideRequest, DriverAssignment, string, RideMatch>(
            request => request.RequestId,
            assignment => assignment.RequestId,
            request => request.Timestamp,
            assignment => assignment.Timestamp,
            (request, assignment) => new RideMatch(
                request.RequestId,
                request.UserId,
                request.Pickup,
                assignment?.DriverId,
                assignment?.VehicleInfo,
                assignment != null,
                assignment != null ? assignment.Timestamp - request.Timestamp : (TimeSpan?)null),
            new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(10),     // Longest a rider waits for a driver
                JoinType = StreamJoinType.Left,
                CleanupInterval = TimeSpan.FromSeconds(30)
            });

        _requests = StreamBuilder<RideRequest>.CreateNewStream("RideMatching")
            .Stream()
            .JoinStream(_join)
            .Sink(match =>
            {
                if (match.IsMatched)
                    riders.DriverAssigned(match.UserId, match.DriverId, match.VehicleInfo);
                else
                    riders.OfferAlternatives(match.UserId, match.Pickup);
            })
            .Build();
        _requests.Start();
    }

    // Called by the mobile app's API
    public void OnRideRequested(RideRequest request) => _requests.Emit(request);

    // Called by the dispatch system's consumer
    public void OnDriverAssigned(DriverAssignment assignment) => _join.ProcessRight(assignment);
}
```

---

### 3. Web Analytics: Click Attribution

Join ad impressions with clicks to calculate click-through rates. A left join reports every impression, clicked or not; a click without a known impression is ignored.

```csharp
public record AdImpression(string ImpressionId, string CampaignId, string UserId, string AdUnit, DateTime Timestamp);
public record AdClick(string ClickId, string ImpressionId, string LandingPage, DateTime Timestamp);
public record AttributedClick(string CampaignId, string AdUnit, string UserId, bool Clicked, TimeSpan? TimeToClick, string LandingPage);

public interface ICampaignMetrics { void Record(AttributedClick attribution); }

public class ClickAttribution
{
    private readonly StreamStreamJoinOperator<AdImpression, AdClick, string, AttributedClick> _join;
    private readonly IStream<AdImpression> _impressions;

    public ClickAttribution(ICampaignMetrics metrics)
    {
        _join = new StreamStreamJoinOperator<AdImpression, AdClick, string, AttributedClick>(
            impression => impression.ImpressionId,
            click => click.ImpressionId,
            impression => impression.Timestamp,
            click => click.Timestamp,
            (impression, click) => new AttributedClick(
                impression.CampaignId,
                impression.AdUnit,
                impression.UserId,
                click != null,
                click != null ? click.Timestamp - impression.Timestamp : (TimeSpan?)null,
                click?.LandingPage),
            new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(30),   // Attribution window
                JoinType = StreamJoinType.Left,          // Every impression, clicked or not
                GracePeriod = TimeSpan.FromMinutes(5)
            });

        _impressions = StreamBuilder<AdImpression>.CreateNewStream("ClickAttribution")
            .Stream()
            .JoinStream(_join)
            .Sink(attribution => metrics.Record(attribution))
            .Build();
        _impressions.Start();
    }

    public void OnImpression(AdImpression impression) => _impressions.Emit(impression);

    public void OnClick(AdClick click) => _join.ProcessRight(click);
}
```

An impression is recorded once per click when its clicks arrive, or once as not clicked when it expires without a click. A matched impression is never reported again as unmatched.

An outer join would also report clicks without an impression, with `impression` set to `null`; the join function above would then throw on `impression.CampaignId`, on the cleanup timer thread. Use an outer join only with a join function that handles both sides being `null`, as in the next example.

---

### 4. IoT: Multi-Sensor Data Fusion

Combine temperature and humidity readings per room for HVAC control. An outer join keeps working when one of the two sensors fails.

```csharp
public record TemperatureReading(string RoomId, double Celsius, DateTime Timestamp);
public record HumidityReading(string RoomId, double Percentage, DateTime Timestamp);
public record RoomClimate(string RoomId, double? Temperature, double? Humidity, double? HeatIndex, string ComfortLevel, DateTime Timestamp);

public interface IHvacController { void Adjust(string roomId, string comfortLevel); }

public class ClimateControl
{
    private readonly StreamStreamJoinOperator<TemperatureReading, HumidityReading, string, RoomClimate> _join;
    private readonly IStream<TemperatureReading> _temperatures;

    public ClimateControl(IHvacController hvac)
    {
        _join = new StreamStreamJoinOperator<TemperatureReading, HumidityReading, string, RoomClimate>(
            temp => temp.RoomId,
            humidity => humidity.RoomId,
            temp => temp.Timestamp,
            humidity => humidity.Timestamp,
            // Either side can be null in an outer join
            (temp, humidity) => new RoomClimate(
                temp?.RoomId ?? humidity.RoomId,
                temp?.Celsius,
                humidity?.Percentage,
                CalculateHeatIndex(temp?.Celsius, humidity?.Percentage),
                DetermineComfortLevel(temp?.Celsius, humidity?.Percentage),
                DateTime.UtcNow),
            new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(5),    // Readings belong together within 5 minutes
                JoinType = StreamJoinType.Outer,         // Keep going when one sensor fails
                CleanupInterval = TimeSpan.FromMinutes(1)
            });

        _temperatures = StreamBuilder<TemperatureReading>.CreateNewStream("ClimateControl")
            .Stream()
            .JoinStream(_join)
            .Filter(climate => climate.ComfortLevel != "Comfortable")
            .Sink(climate => hvac.Adjust(climate.RoomId, climate.ComfortLevel))
            .Build();
        _temperatures.Start();
    }

    // Temperature sensors (for example one MQTT topic)
    public void OnTemperature(TemperatureReading reading) => _temperatures.Emit(reading);

    // Humidity sensors (another MQTT topic)
    public void OnHumidity(HumidityReading reading) => _join.ProcessRight(reading);

    private static double? CalculateHeatIndex(double? temp, double? humidity)
    {
        if (temp == null || humidity == null) return null;
        // Simplified heat index formula
        return temp.Value + (0.5 * humidity.Value);
    }

    private static string DetermineComfortLevel(double? temp, double? humidity)
    {
        if (temp == null || humidity == null) return "Unknown";
        if (temp < 18) return "Too Cold";
        if (temp > 26) return "Too Hot";
        if (humidity < 30) return "Too Dry";
        if (humidity > 60) return "Too Humid";
        return "Comfortable";
    }
}
```

Every temperature reading is combined with every humidity reading of the same room in the window, so a room that reports every minute produces several results per reading.

---

### 5. Financial: Trade Execution Matching

Match trade orders with their executions for compliance reporting.

```csharp
public record TradeOrder(string OrderId, string Symbol, int Quantity, decimal Price, string Side, DateTime Timestamp);
public record TradeExecution(string ExecutionId, string OrderId, int FilledQty, decimal AvgPrice, DateTime Timestamp);
public record TradeReport(
    string OrderId,
    string Symbol,
    string Side,
    int OrderedQty,
    int? FilledQty,
    decimal OrderPrice,
    decimal? ExecutionPrice,
    decimal? Slippage,
    TimeSpan? ExecutionTime,
    string Status);

public interface IComplianceDesk
{
    void Log(TradeReport report);
    void EscalateUnexecuted(TradeReport report);
    void FlagForReview(TradeReport report);
}

public class TradeCompliance
{
    private readonly StreamStreamJoinOperator<TradeOrder, TradeExecution, string, TradeReport> _join;
    private readonly IStream<TradeOrder> _orders;

    public TradeCompliance(IComplianceDesk desk)
    {
        _join = new StreamStreamJoinOperator<TradeOrder, TradeExecution, string, TradeReport>(
            order => order.OrderId,
            execution => execution.OrderId,
            order => order.Timestamp,
            execution => execution.Timestamp,
            (order, execution) => new TradeReport(
                order.OrderId,
                order.Symbol,
                order.Side,
                order.Quantity,
                execution?.FilledQty,
                order.Price,
                execution?.AvgPrice,
                execution != null ? Math.Abs(execution.AvgPrice - order.Price) : (decimal?)null,
                execution != null ? execution.Timestamp - order.Timestamp : (TimeSpan?)null,
                execution == null ? "Pending" : execution.FilledQty == order.Quantity ? "Filled" : "Partial"),
            new StreamJoinConfiguration
            {
                WindowSize = TimeSpan.FromMinutes(15),   // Orders should execute within 15 minutes
                JoinType = StreamJoinType.Left,          // Report orders that never execute
                GracePeriod = TimeSpan.FromSeconds(30)
            });

        _orders = StreamBuilder<TradeOrder>.CreateNewStream("TradeCompliance")
            .Stream()
            .JoinStream(_join)
            .Sink(report =>
            {
                desk.Log(report);

                if (report.Status == "Pending")
                    desk.EscalateUnexecuted(report);
                else if (report.Slippage > 0.05m)   // More than 5 cents of slippage
                    desk.FlagForReview(report);
            })
            .Build();
        _orders.Start();
    }

    public void OnOrder(TradeOrder order) => _orders.Emit(order);

    public void OnExecution(TradeExecution execution) => _join.ProcessRight(execution);
}
```

Each partial fill is a separate execution, so an order filled in three parts produces three reports.

---

## Configuration Options

### StreamJoinConfiguration

```csharp
var config = new StreamJoinConfiguration
{
    // How long an event can wait for a match (default: 5 minutes)
    WindowSize = TimeSpan.FromMinutes(10),

    // Join semantics: Inner (default), Left, Right or Outer
    JoinType = StreamJoinType.Inner,

    // How often expired events are removed and unmatched ones emitted (default: 30 seconds)
    CleanupInterval = TimeSpan.FromSeconds(30),

    // Extra time before an expired event is removed (default: zero)
    GracePeriod = TimeSpan.FromSeconds(10),

    // Events kept per key and side; the oldest is dropped beyond this (default: 1000)
    MaxBufferSizePerKey = 1000
};
```

The cleanup timer starts when the operator is created, with the configuration's `CleanupInterval`.

### Factory Methods

```csharp
var innerConfig = StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(5));
var leftConfig = StreamJoinConfiguration.LeftJoin(TimeSpan.FromMinutes(5));
var outerConfig = StreamJoinConfiguration.OuterJoin(TimeSpan.FromMinutes(5));
var defaultConfig = StreamJoinConfiguration.Default;   // Inner join, 5-minute window
```

There is no factory for a right join; set `JoinType = StreamJoinType.Right`.

---

## Architecture Patterns

### Pattern 1: Both Sides from Kafka

The left side is a stream with a Kafka source. The right side is a second stream whose sink feeds the join operator.

```csharp
public record OrderEvent(string OrderId, decimal Amount, DateTime Timestamp);
public record PaymentEvent(string OrderId, string Method, DateTime Timestamp);
public record OrderPayment(OrderEvent Order, PaymentEvent Payment);

public static class KafkaJoinSetup
{
    public static (IStream<OrderEvent> Orders, IStream<PaymentEvent> Payments) Create(string bootstrapServers, Action<OrderPayment> process)
    {
        var joinOp = new StreamStreamJoinOperator<OrderEvent, PaymentEvent, string, OrderPayment>(
            order => order.OrderId,
            payment => payment.OrderId,
            order => order.Timestamp,
            payment => payment.Timestamp,
            (order, payment) => new OrderPayment(order, payment),
            StreamJoinConfiguration.InnerJoin(TimeSpan.FromMinutes(30)));

        var orders = StreamBuilder<OrderEvent>.CreateNewStream("Orders")
            .Stream(new KafkaSourceOperator<OrderEvent>(bootstrapServers, "orders-topic"))
            .JoinStream(joinOp)
            .Sink(process)
            .Build();

        // A second stream consumes payments and feeds the right side
        var payments = StreamBuilder<PaymentEvent>.CreateNewStream("Payments")
            .Stream(new KafkaSourceOperator<PaymentEvent>(bootstrapServers, "payments-topic"))
            .Sink(payment => joinOp.ProcessRight(payment))
            .Build();

        return (orders, payments);
    }
}
```

`KafkaSourceOperator<T>` is in the `Cortex.Streams.Kafka` package and namespace. Start both streams with `Start()`.

### Pattern 2: HTTP Webhook Integration

In an ASP.NET Core application, a webhook endpoint feeds the right side:

```csharp
var joinOp = new StreamStreamJoinOperator<InternalEvent, WebhookEvent, string, Result>(/* ... */);

var internalStream = StreamBuilder<InternalEvent>.CreateNewStream("Internal")
    .Stream()
    .JoinStream(joinOp)
    .Sink(result => Process(result))
    .Build();
internalStream.Start();

app.MapPost("/webhook", (WebhookEvent webhookEvent) =>
{
    joinOp.ProcessRight(webhookEvent);
    return Results.Ok();
});
```

`InternalEvent`, `WebhookEvent`, `Result` and `Process` stand for your own types and method. Matches for a webhook are processed inside the HTTP request, so keep the sink fast.

---

## Best Practices

1. **Choose window sizes from your latencies:**
   - Too small: legitimate matches are missed.
   - Too large: more memory, and unmatched events are reported later.

2. **Use UTC timestamps that are close to the current time.** Expiry is measured against `DateTime.UtcNow`, so local times or old event times make events expire early or never match.

3. **Allow for late data:**
   ```csharp
   GracePeriod = TimeSpan.FromMinutes(2)  // Wait 2 more minutes before reporting unmatched events
   ```

4. **Monitor buffer sizes:**
   ```csharp
   Console.WriteLine($"Left buffer: {joinOp.GetLeftBufferCount()}, Right buffer: {joinOp.GetRightBufferCount()}");
   ```

5. **Choose the join type deliberately:**
   - `Inner`: only matched pairs matter.
   - `Left`: every left event must be reported.
   - `Right`: every right event must be reported.
   - `Outer`: every event from both sides must be reported.

6. **Handle `null` in the join function** for left, right and outer joins:
   ```csharp
   (left, right) => new Result(
       left?.Id ?? right.RefId,   // One side is null for unmatched events
       left?.Value,
       right?.Value)
   ```
   An exception in the join function for an unmatched event happens on the cleanup timer thread, where no caller can catch it; an unhandled exception on a timer thread terminates the process.

7. **Keep sinks thread-safe.** Results arrive from the thread that calls `Emit`, the threads that call `ProcessRight` and the cleanup timer.

8. **Dispose when done:**
   ```csharp
   // Stops the cleanup timer
   joinOp.Dispose();
   ```
   Disposing the stream does not dispose the join operator.

---

## Memory Considerations

| Factor | Impact | Mitigation |
|--------|--------|------------|
| Window size | Larger means more memory | Use the smallest window that meets your requirements |
| Event rate | Higher means more memory | Filter or pre-aggregate before the join |
| Key cardinality | More keys means more memory | Expired keys are removed every `CleanupInterval` |
| Events per key | Unbounded without a limit | `MaxBufferSizePerKey` drops the oldest event of a key and side |
| Event size | Larger events mean more memory | Join on small records with only the fields you need |

```csharp
// Memory-conscious configuration
var config = new StreamJoinConfiguration
{
    WindowSize = TimeSpan.FromMinutes(5),
    MaxBufferSizePerKey = 100,                   // Limit per key and side
    CleanupInterval = TimeSpan.FromSeconds(10)   // Frequent cleanup
};
```

---

## See Also

- [Stream-Table Joins](Stream-Table-Joins.md) - for joining streams with reference tables
- [Branches, Merge and Dynamic Branching](Branch-and-Merge.md) - for splitting and recombining a stream
- [Stream Performance](stream-performance.md) - for buffered and async processing
