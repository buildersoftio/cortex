# Stream-Table Joins in Cortex Data Framework

This guide covers how to use **Stream-Table Joins** in Cortex Data Framework to enrich streaming data with reference data stored in state stores.

## Overview

A stream-table join enriches each event of a stream by looking up related data in a table (a state store). Use it when you have:
- A stream of events (orders, clicks, transactions)
- A table of reference data (customers, products, configurations)

Cortex.Streams provides two stream-table joins:

| Join type | Behavior | Use case |
|-----------|----------|----------|
| **Inner join** (`Join`) | Emits only when the key exists in the table | The enrichment data is required |
| **Left join** (`LeftJoin`) | Always emits; the table value is `null` (`default`) when the key is missing | The enrichment data is optional |

For each event, the join extracts a key, looks it up in the table at that moment and passes the event and the table value to your join function. Later changes to the table do not affect events that were already joined.

The examples on this page use these namespaces:

```csharp
using Cortex.States;     // InMemoryStateStore, IDataStore
using Cortex.Streams;
```

---

## Inner Join (Stream-Table)

The inner join emits a result only when the event's key exists in the table.

### Example: Order Processing with Required Customer Data

```csharp
// Define models
public record Order(string OrderId, int CustomerId, decimal Amount, DateTime Timestamp);
public record Customer(int Id, string Name, string Email, string Tier);
public record EnrichedOrder(string OrderId, decimal Amount, string CustomerName, string CustomerTier);
```

```csharp
// Create and populate the customer table
var customerStore = new InMemoryStateStore<int, Customer>("CustomerStore");
customerStore.Put(1001, new Customer(1001, "Alice Smith", "alice@example.com", "Gold"));
customerStore.Put(1002, new Customer(1002, "Bob Johnson", "bob@example.com", "Silver"));
customerStore.Put(1003, new Customer(1003, "Carol Williams", "carol@example.com", "Bronze"));

// Build the stream with an inner join
var orderStream = StreamBuilder<Order>.CreateNewStream("OrderEnrichmentStream")
    .Stream()
    .Join(
        customerStore,
        order => order.CustomerId,  // Key selector
        (order, customer) => new EnrichedOrder(
            order.OrderId,
            order.Amount,
            customer.Name,
            customer.Tier))
    .Sink(enrichedOrder => Console.WriteLine(
        $"Processing order {enrichedOrder.OrderId} for {enrichedOrder.CustomerName} ({enrichedOrder.CustomerTier})"))
    .Build();

// Start and emit orders
orderStream.Start();
orderStream.Emit(new Order("ORD-001", 1001, 150.00m, DateTime.UtcNow)); // Emits: Alice exists
orderStream.Emit(new Order("ORD-002", 1002, 75.50m, DateTime.UtcNow));  // Emits: Bob exists
orderStream.Emit(new Order("ORD-003", 9999, 200.00m, DateTime.UtcNow)); // Dropped: customer 9999 not found
```

**Output:**
```
Processing order ORD-001 for Alice Smith (Gold)
Processing order ORD-002 for Bob Johnson (Silver)
```

> **Note:** Order ORD-003 is dropped without an error because customer 9999 is not in the table. Use a left join if you need to see such orders.

---

## Left Join (Stream-Table)

The left join emits a result for **every** event, even when the table has no entry for its key. Then the table value passed to the join function is `default`, which is `null` for classes and records.

### Example: IoT Sensor Data with Optional Device Metadata

```csharp
// Define models
public record SensorReading(string SensorId, double Value, string Unit, DateTime Timestamp);
public record DeviceInfo(string SensorId, string Location, string Owner, DateTime InstalledAt);
public record EnrichedReading(string SensorId, double Value, string Unit, string Location, string Owner, bool HasDeviceInfo);
```

```csharp
// Device registry: not every sensor is registered
var deviceRegistry = new InMemoryStateStore<string, DeviceInfo>("DeviceRegistry");
deviceRegistry.Put("SENSOR-001", new DeviceInfo("SENSOR-001", "Building A, Floor 2", "Facilities Team", new DateTime(2023, 1, 15)));
deviceRegistry.Put("SENSOR-002", new DeviceInfo("SENSOR-002", "Building B, Floor 1", "IT Department", new DateTime(2023, 3, 20)));
// Note: SENSOR-003 is NOT registered

// Build the stream with a left join
var sensorStream = StreamBuilder<SensorReading>.CreateNewStream("SensorEnrichmentStream")
    .Stream()
    .LeftJoin(
        deviceRegistry,
        reading => reading.SensorId,
        (reading, device) => new EnrichedReading(
            reading.SensorId,
            reading.Value,
            reading.Unit,
            device?.Location,      // May be null
            device?.Owner,         // May be null
            device != null))       // Whether device info was found
    .Sink(enriched =>
    {
        if (enriched.HasDeviceInfo)
            Console.WriteLine($"[{enriched.Location}] {enriched.SensorId}: {enriched.Value} {enriched.Unit}");
        else
            Console.WriteLine($"[UNKNOWN DEVICE] {enriched.SensorId}: {enriched.Value} {enriched.Unit} - Please register this device!");
    })
    .Build();

// Start and emit sensor readings
sensorStream.Start();
sensorStream.Emit(new SensorReading("SENSOR-001", 23.5, "C", DateTime.UtcNow));      // Has device info
sensorStream.Emit(new SensorReading("SENSOR-002", 45.2, "%", DateTime.UtcNow));      // Has device info
sensorStream.Emit(new SensorReading("SENSOR-003", 1013.25, "hPa", DateTime.UtcNow)); // Emitted without device info
```

**Output** (numbers are formatted with the current culture):
```
[Building A, Floor 2] SENSOR-001: 23.5 C
[Building B, Floor 1] SENSOR-002: 45.2 %
[UNKNOWN DEVICE] SENSOR-003: 1013.25 hPa - Please register this device!
```

---

## Real-World Use Cases

Each use case declares the records and service interfaces it uses, so you can paste it into a project that references `Cortex.Streams` and plug in your own implementations.

### 1. E-Commerce: Product Catalog Enrichment

```csharp
public record CartItem(string SessionId, string ProductSku, int Quantity);
public record Product(string Sku, string Name, decimal Price, int StockLevel);
public record EnrichedCartItem(string SessionId, string ProductName, decimal UnitPrice, decimal TotalPrice, bool InStock);

public interface ICartService { void Process(EnrichedCartItem item); }

public class CartEnrichment
{
    public CartEnrichment(IDataStore<string, Product> productCatalog, ICartService carts)
    {
        Stream = StreamBuilder<CartItem>.CreateNewStream("CartEnrichment")
            .Stream()
            .LeftJoin(
                productCatalog,
                item => item.ProductSku,
                (item, product) => new EnrichedCartItem(
                    item.SessionId,
                    product?.Name ?? "Unknown Product",
                    product?.Price ?? 0m,
                    (product?.Price ?? 0m) * item.Quantity,
                    product?.StockLevel > 0))
            .Filter(item => item.InStock)  // Only in-stock items; unknown products are never in stock
            .Sink(item => carts.Process(item))
            .Build();
    }

    public IStream<CartItem> Stream { get; }
}
```

Load the catalog into an `InMemoryStateStore<string, Product>` (or any other `IDataStore<string, Product>`) before starting the stream.

### 2. Financial Services: Transaction Risk Scoring

```csharp
public record Transaction(string TxId, string AccountId, decimal Amount, string MerchantCategory);
public record AccountProfile(string AccountId, string RiskLevel, decimal DailyLimit, List<string> TrustedCategories);
public record ScoredTransaction(string TxId, decimal Amount, string RiskLevel, bool ExceedsLimit, bool TrustedMerchant);

public interface IFraudTeam { void Alert(ScoredTransaction transaction); }

public class TransactionScoring
{
    public TransactionScoring(IDataStore<string, AccountProfile> accountProfiles, IFraudTeam fraudTeam)
    {
        Stream = StreamBuilder<Transaction>.CreateNewStream("TransactionScoring")
            .Stream()
            .LeftJoin(
                accountProfiles,
                tx => tx.AccountId,
                (tx, profile) => new ScoredTransaction(
                    tx.TxId,
                    tx.Amount,
                    profile?.RiskLevel ?? "UNKNOWN",  // Flag unknown accounts
                    tx.Amount > (profile?.DailyLimit ?? 0),
                    profile?.TrustedCategories?.Contains(tx.MerchantCategory) ?? false))
            .Filter(scored => scored.RiskLevel == "UNKNOWN" || scored.ExceedsLimit || !scored.TrustedMerchant)
            .Sink(scored => fraudTeam.Alert(scored))
            .Build();
    }

    public IStream<Transaction> Stream { get; }
}
```

### 3. Gaming: Player Event Enrichment

```csharp
public record GameEvent(string PlayerId, string EventType, Dictionary<string, object> Data);
public record PlayerProfile(string PlayerId, int Level, string Rank, bool IsPremium, DateTime JoinedAt);
public record EnrichedGameEvent(string PlayerId, string EventType, int PlayerLevel, bool IsPremium, Dictionary<string, object> Data);

public interface IGameAnalytics
{
    void SendToPremium(EnrichedGameEvent gameEvent);
    void SendToStandard(EnrichedGameEvent gameEvent);
}

public class GameEventEnrichment
{
    public GameEventEnrichment(IDataStore<string, PlayerProfile> playerProfiles, IGameAnalytics analytics)
    {
        Stream = StreamBuilder<GameEvent>.CreateNewStream("GameEventEnrichment")
            .Stream()
            .LeftJoin(
                playerProfiles,
                evt => evt.PlayerId,
                (evt, player) => new EnrichedGameEvent(
                    evt.PlayerId,
                    evt.EventType,
                    player?.Level ?? 0,
                    player?.IsPremium ?? false,
                    evt.Data))
            // Route to different analytics pipelines based on player status
            .AddBranch("premium", evt => evt.IsPremium, b => b.Sink(evt => analytics.SendToPremium(evt)))
            .AddBranch("standard", evt => !evt.IsPremium, b => b.Sink(evt => analytics.SendToStandard(evt)))
            .Build();
    }

    public IStream<GameEvent> Stream { get; }
}
```

The two conditional branches replace an `if` inside a sink, so each pipeline shows up in `GetBranches()` and telemetry. See [Branches, Merge and Dynamic Branching](Branch-and-Merge.md).

---

## Keeping the Table Updated

The table can change while the stream is running; each event sees the value at the moment it is joined. `InMemoryStateStore` is safe to update from another thread.

```csharp
public record CustomerOrder(string OrderId, int CustomerId, decimal Amount);
public record CustomerOrderView(string OrderId, string CustomerName);

public interface ICustomerChangeFeed
{
    // For example change data capture (CDC) from the customer database
    Task<IReadOnlyList<Customer>> FetchChangesAsync(CancellationToken cancellationToken);
}

public class CustomerTableSync
{
    private readonly InMemoryStateStore<int, Customer> _customers = new InMemoryStateStore<int, Customer>("CustomerStore");

    public CustomerTableSync(Action<CustomerOrderView> process)
    {
        Stream = StreamBuilder<CustomerOrder>.CreateNewStream("Orders")
            .Stream()
            .LeftJoin(_customers, o => o.CustomerId, (o, c) => new CustomerOrderView(o.OrderId, c?.Name ?? "unknown customer"))
            .Sink(process)
            .Build();
    }

    public IStream<CustomerOrder> Stream { get; }

    // Run in the background, for example from a hosted service
    public async Task KeepUpdatedAsync(ICustomerChangeFeed feed, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var customer in await feed.FetchChangesAsync(cancellationToken))
            {
                _customers.Put(customer.Id, customer);
            }
            await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
        }
    }
}
```

---

## Using Different State Store Backends

A join accepts any `IDataStore<TKey, TValue>`:

```csharp
// In-memory (default, fast, not persistent)
var memoryStore = new InMemoryStateStore<string, Product>("Products");

// RocksDB (persistent, good for large tables); package and namespace Cortex.States.RocksDb
var rocksStore = new RocksDbStateStore<string, Product>("Products", "/data/rocksdb");

// Use any store with joins
var stream = StreamBuilder<CartItem>.CreateNewStream("Cart")
    .Stream()
    .LeftJoin(rocksStore, item => item.ProductSku, (item, product) => new { item.SessionId, product?.Name })
    .Sink(x => Console.WriteLine($"{x.SessionId}: {x.Name}"))
    .Build();
```

---

## Best Practices

1. **Choose the right join type:**
   - Use `Join` (inner) when the reference data is required.
   - Use `LeftJoin` when the reference data is optional.

2. **Handle `null` in left joins:**
   ```csharp
   .LeftJoin(store, keySelector, (left, right) => new Result(
       left.Id,
       right?.Name ?? "Unknown",
       right?.Value ?? defaultValue))
   ```

3. **Fill tables before starting the stream** when you can, so the first events are not dropped (inner join) or emitted without data (left join).

4. **Match the update frequency to the join:**
   - For slowly changing data, batch updates are fine.
   - For data that changes as fast as the stream, consider a [Stream-Stream Join](Stream-Stream-Joins.md).

5. **Monitor table size**: an in-memory table keeps every entry in memory.

---

## See Also

- [Stream-Stream Windowed Joins](Stream-Stream-Joins.md) - for joining two unbounded streams
- [Branches, Merge and Dynamic Branching](Branch-and-Merge.md) - for routing enriched events
- [Stream Performance](stream-performance.md) - for buffered and async processing
