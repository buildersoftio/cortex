# Branches, Merge and Dynamic Branching

Cortex.Streams can split a stream into named branches, let a branch accept only some items, merge branch outputs back into one stream, and add or remove branches while the stream runs.

## Table of Contents

- [Overview](#overview)
- [Conditional Branches](#conditional-branches)
- [Merging Branches](#merging-branches)
- [Dynamic Branching](#dynamic-branching)
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

## Conditional Branches

Pass a predicate before the branch configuration. Items that do not match skip the branch; the main pipeline is unaffected.

```csharp
var stream = StreamBuilder<Order>.CreateNewStream("Orders")
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
var stream = StreamBuilder<SensorReading>.CreateNewStream("Sensors")
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
var stream = StreamBuilder<Order>.CreateNewStream("Orders")
    .Stream()
    .AddBranch("audit", b => b.Sink(order => auditLog.Write(order)))
    .Sink(order => repository.Save(order))
    .Build();

stream.Start();

// Later: start sending VIP orders to a CRM, then stop
stream.AddBranch<Order>("vip", order => order.Customer.IsVip, b => b
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
