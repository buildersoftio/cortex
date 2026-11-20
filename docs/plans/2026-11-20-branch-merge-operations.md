# Plan: Branch merge and dynamic branching for Cortex.Streams (issue #155)
Status: in-progress
Date: 2026-11-20 · Agent: lead-developer · Size: large · Base: v3.2/release@47f87da · Branch: v3/feature/155
Approved: 2026-11-20 via chat (plan card GO)
Consent: Review panel (2026-11-20)
Execution: inline
Request: "In our journey of releasing v3.2 we need to work in the next feature, the documentation of it is at folloing issue #155 in github, lets analyise it and plan its implementation, create the PR to merge it into v3.2/release branch."

## Goal
A Cortex.Streams pipeline can merge the outputs of several (optionally conditional) branches back into one continuing stream, and a built stream can add and remove branches at runtime through a thread-safe fork, with tests proving each behaviour and a PR open against v3.2/release.
## Context
- Issue #155 (https://github.com/buildersoftio/cortex/issues/155), verbatim: "Branch merging – after processing in different branches, users may need to merge results. Provide a Merge operator that takes multiple branches and combines their outputs into a single stream. Dynamic branching – allow branches to be added or removed at runtime. Expose a RemoveBranch method and ensure that branch operator dictionaries are thread‑safe, and they can be conditional." GitHub milestone is "Cortex v3.5"; the user targets v3.2/release.
- Fork storage is a plain `Dictionary` (src/Cortex.Streams/Operators/ForkOperator.cs:15), enumerated on the hot path (:122, :148) and mutated by `AddBranch`, which silently replaces a same-named branch (:89-96). `Branches` exposes the live dictionary (:182). There is no remove.
- `AddBranch` telemetry/error-handling propagation to a late branch only reaches the `BranchOperator` and its first operator (ForkOperator.cs:98-108, BranchOperator.cs:60-76); `Stream` walks the full graph only once, in its constructor (src/Cortex.Streams/Stream.cs:53-56, :58-113).
- `Stream.InitializeTelemetry` / `InitializeErrorHandling` recurse without a visited set (Stream.cs:58-113); `CollectStateStores` already uses one (:384-412). A merge point reachable from several branches needs the visited set.
- Builder branching: src/Cortex.Streams/StreamBuilder.cs:234-276. One fork per builder instance; operators chained after `AddBranch` become the fork's continuation (ForkOperator.cs:158-167). Type-changing operators create a new builder without `_branchOperators` (StreamBuilder.cs:101), so `Stream.GetBranches()` (Stream.cs:366-374) loses branches declared before a `Map` — pre-existing.
- Branch builder: src/Cortex.Streams/BranchStreamBuilder.cs:19-20 (internal `_firstOperator`, `_lastOperator`), `Sink` returns void (:84-118). `BranchOperator.SetNext` throws (BranchOperator.cs:113-116).
- Conditional routing pattern to reuse: FanOut builds `FilterOperator` → sink inside a branch (src/Cortex.Streams/FanOutBuilder.cs:94-110).
- Public surface touched: `IStream<TIn>` (src/Cortex.Streams/Abstractions/IStream.cs:10-79), `IStreamBuilder<TIn,TCurrent>` (src/Cortex.Streams/Abstractions/IStreamBuilder.cs:27-110).
- Targets net10.0;net9.0;net8.0;net7.0;netstandard2.1 with `LangVersion 8.0` (src/Cortex.Streams/Cortex.Streams.csproj:4-5): no records, `is not`, target-typed `new`, or init-only setters.
- No `InternalsVisibleTo` in Cortex.Streams: tests go through the public API. Tests live in src/Cortex.Tests/Streams/Tests (namespace `Cortex.Tests.Streams.Tests`, xUnit + Moq, TelemetryTests.cs:6-15).
- Known pre-existing failures: 4 Cortex.Streams tests (issue #238: two SourceOperatorErrorHandlingTests, two SessionWindowOperatorTests). The full-suite run is reported with these named separately.
- Project rules (CONTRIBUTING.md:77-78): "Ensure your code adheres to the existing style and conventions" and "Include or update tests for your changes."
- Branch naming follows the repo's `v3/feature/<issue>` convention (e.g. v3/feature/214).
- User choices (2026-11-20, info.request): runtime add attaches to the fork selected by element type; add a wiki page; review panel.
- Assumption: "they can be conditional" means a branch may carry a predicate that decides which items enter it, both at build time and at runtime.
- Assumption: merging happens inside one pipeline; merging two separately built `IStream` instances is out of scope.
## Approach
Make `ForkOperator<T>` copy-on-write: mutations take a lock and publish a new ordered `BranchOperator<T>[]` snapshot; `Process` reads the snapshot once, so in-flight items finish on the old set and no enumeration can be invalidated. Add `TryAddBranch` (rejects duplicates) and `RemoveBranch`, and an internal non-generic `IForkOperator` (element type, branch infos, remove) so `Stream` can find forks by walking the operator graph. `IStream<TIn>` gains `AddBranch<TBranch>(name, config)`, `AddBranch<TBranch>(name, condition, config)` and `bool RemoveBranch(name)`; runtime add targets the single non-merge fork whose element type is `TBranch` (none or several → `InvalidOperationException`), initialises telemetry and error handling on the whole new branch subgraph before publishing it, and `GetBranches()` is rebuilt from the live graph (fixing the lost-after-Map case). Conditional branches reuse the FanOut pattern: a `FilterOperator<T>(condition)` heads the branch chain, so error handling and telemetry come for free; `IStreamBuilder` gains `AddBranch(name, condition, config)`. Merge adds `IStreamBuilder.Merge<TNext>(Action<IMergeBuilder<TIn,TCurrent,TNext>>)`, where each `Branch(name[, condition], b => …)` returns the branch builder whose last operator is wired into a shared `MergeOperator<TNext>`; the merge forwards each branch output, in branch order, to whatever is chained after it, and an empty branch (`b => b`) passes items straight through.
Rejected: `ConcurrentDictionary` — no stable branch order and enumeration still races with remove semantics; the snapshot array is cheaper on the hot path.
Rejected: a lock around `Process` — serialises the whole pipeline and deadlocks when a branch re-enters `AddBranch`.
Rejected: named fork points for runtime add — new concept and more API; the user chose type-selected fork with an ambiguity error.
Rejected: a lock inside `MergeOperator` — every other operator is unsynchronised; serialising only after a merge would be inconsistent and slow.
Rejected: changing build-time duplicate-name behaviour (silent replace) — existing behaviour; only the new runtime API rejects duplicates.
## Out of scope
- Merging separately built `IStream` instances; runtime add of merge branches (remove works on any branch); nested merges inside a branch builder.
- Fixing the 4 known failing tests (issue #238); changing FanOut semantics; package version bumps; CI changes.
- NativeAOT/trimming checks for Cortex.Streams.
## Verification
- `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release` → exit 0, "0 Error(s)" for all five target frameworks.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests|FullyQualifiedName~StreamBuilderTests|FullyQualifiedName~FanOut|FullyQualifiedName~TelemetryTests|FullyQualifiedName~ErrorHandlingTests"` → "Failed: 0", exit 0.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release` → only the 4 issue-#238 tests fail; any other failure blocks the PR.
- End to end: a test builds `Stream → Merge(two branches mapping to string, one conditional) → Sink`, emits values, then at runtime adds a conditional branch and removes another while `EmitAsync` runs concurrently, and asserts exact sink contents with no exception.
## Orchestration
- Consent: Review panel (2026-11-20) · Estimate: 4 agents, 2 waves, about 38 cards (3 extra), ≈2.4× Solo tokens
- Companions: researcher=researcher · software-architect=software-architect · developer=developer · code-reviewer=— (stand-in; not on the allowed delegation list) · security-reviewer=— · qa-engineer=qa-engineer · debugger=— · ui-ux-designer=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Review | reviewer stand-in (correctness), reviewer stand-in (spec), qa-engineer (gaps) | read-only, code tools, 25 | READ_FIRST sophon-scratch-review.diff | FINDINGS ≥80, diff only; I refute each Critical/Important from the code | pending |
| 2 | Critic | reviewer stand-in (completeness) | read-only, code tools, 15 | request, Goal, Verification, commits | each item checked by me | pending |
## Tasks
### Task 1: Thread-safe fork with runtime AddBranch and RemoveBranch
- [x] Done
Files: `src/Cortex.Streams/Operators/ForkOperator.cs` (modify), `src/Cortex.Streams/Operators/IForkOperator.cs` (create), `src/Cortex.Streams/Abstractions/IStream.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Tests/Streams/Tests/DynamicBranchingTests.cs` (create)
Depends on: none
- [x] Add `IStream.AddBranch<TBranch>(string name, Action<IBranchStreamBuilder<TIn,TBranch>> config)` and `bool RemoveBranch(string name)` with XML docs; implement in `Stream` (find forks via graph walk with a visited set; none or several matching → `InvalidOperationException`; duplicate name → `ArgumentException`; branch with no operators → `InvalidOperationException`, as StreamBuilder.cs:266-269).
- [x] Write DynamicBranchingTests: added branch receives only later items; removed branch stops receiving; `RemoveBranch` of unknown name returns false; duplicate/no-fork/ambiguous errors; runtime branch gets error handling (a throwing Map with the Skip strategy does not stop the stream); `GetBranches()` reflects add/remove and still lists branches declared before a `Map`; re-entrant add from inside a branch sink; 4 threads `EmitAsync` while another adds/removes 100 times with no exception.
- [x] Run the tests red: the re-entrant test fails with "Collection was modified" on the dictionary fork.
- [x] Make `ForkOperator` copy-on-write (lock + `Volatile` snapshot array, ordered), add `TryAddBranch`/`RemoveBranch`, implement `IForkOperator`; `Branches` returns a snapshot copy; `GetBranches()` walks the graph; visited sets in `InitializeTelemetry`/`InitializeErrorHandling`.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~StreamBuilderTests|FullyQualifiedName~FanOut|FullyQualifiedName~TelemetryTests"`
Expected: build exit 0 "0 Error(s)"; tests "Failed: 0", exit 0
Commit: feat(streams): add runtime AddBranch and RemoveBranch with thread-safe fork
### Task 2: Conditional branches at build time and runtime
- [ ] Done
Files: `src/Cortex.Streams/Abstractions/IStreamBuilder.cs` (modify), `src/Cortex.Streams/StreamBuilder.cs` (modify), `src/Cortex.Streams/Abstractions/IStream.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Tests/Streams/Tests/BranchMergeTests.cs` (create), `src/Cortex.Tests/Streams/Tests/DynamicBranchingTests.cs` (modify)
Depends on: Task 1
- [ ] Write tests first: `AddBranch(name, condition, config)` routes only matching items and the main path still gets all; null condition → `ArgumentNullException`; a throwing condition follows the stream's error strategy; runtime `AddBranch<T>(name, condition, config)` routes only matching items. Run red (compile error on the missing overloads).
- [ ] Add `IStreamBuilder.AddBranch(string name, Func<TCurrent,bool> condition, Action<IBranchStreamBuilder<TIn,TCurrent>> config)` and the `IStream` runtime overload; both prepend a `FilterOperator<T>(condition)` to the branch chain (FanOutBuilder.cs:102-107 pattern) through one shared helper.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests|FullyQualifiedName~StreamBuilderTests|FullyQualifiedName~ErrorHandlingTests"`
Expected: build exit 0 "0 Error(s)"; tests "Failed: 0", exit 0
Commit: feat(streams): support conditional branches
### Task 3: Merge operator combining branch outputs into one stream
- [ ] Done
Files: `src/Cortex.Streams/Operators/MergeOperator.cs` (create), `src/Cortex.Streams/Abstractions/IMergeBuilder.cs` (create), `src/Cortex.Streams/MergeBuilder.cs` (create), `src/Cortex.Streams/Abstractions/IStreamBuilder.cs` (modify), `src/Cortex.Streams/StreamBuilder.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Tests/Streams/Tests/BranchMergeTests.cs` (modify)
Depends on: Task 2
- [ ] Write tests first: two branches mapping `int` to `string` merge into one sink in branch order per item; a conditional merge branch; `b => b` pass-through branch; `Map`/`Filter` chained after the merge; merged branches listed by `GetBranches()`; telemetry initialised once per downstream operator; error strategy reaches operators after the merge; state stores in branches and after the merge are found by `GetStateStoreByName`; no branches → `InvalidOperationException`; duplicate branch name → `ArgumentException`; a config returning an unrelated builder → `ArgumentException`; `RemoveBranch` on a merge branch stops its output. Run red (compile error on missing `Merge`).
- [ ] Add `MergeOperator<T>` (`IOperator`, `IHasNextOperators`, `ITelemetryEnabled`, forwards to next), `IMergeBuilder<TIn,TCurrent,TNext>` with `Branch(name, config)` and `Branch(name, condition, config)` where config is `Func<IBranchStreamBuilder<TIn,TCurrent>, IBranchStreamBuilder<TIn,TNext>>`, `MergeBuilder`, and `IStreamBuilder.Merge<TNext>(Action<IMergeBuilder<…>>)` returning `IStreamBuilder<TIn,TNext>`; mark the merge's fork as non-targetable for runtime add.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests|FullyQualifiedName~StreamBuilderTests|FullyQualifiedName~FanOut|FullyQualifiedName~TelemetryTests|FullyQualifiedName~ErrorHandlingTests"`
Expected: build exit 0 "0 Error(s)"; tests "Failed: 0", exit 0
Commit: feat(streams): add Merge operator to combine branch outputs
### Task 4: Document branching, merging and dynamic branches
- [ ] Done
Files: `docs/wiki/Branch-and-Merge.md` (create)
Depends on: Task 3
- [ ] Write the wiki page in the style of docs/wiki/FanOut-Multiple-Sinks.md: conditional branches, Merge, runtime AddBranch/RemoveBranch, thread-safety and ordering guarantees, the fork-selection rule and its errors; every snippet uses the signatures from Tasks 1-3.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests|FullyQualifiedName~StreamBuilderTests|FullyQualifiedName~FanOut|FullyQualifiedName~TelemetryTests|FullyQualifiedName~ErrorHandlingTests"`
Expected: build exit 0 "0 Error(s)"; tests "Failed: 0", exit 0
Commit: docs(streams): document branch merge and dynamic branching
## Risks
- Adding members to public `IStream<TIn>` and `IStreamBuilder<TIn,TCurrent>` breaks third-party implementations of those interfaces; called out in the PR.
- Concurrency tests can be timing-dependent; they assert absence of exceptions and final counts with joins, never sleeps.
- An item already inside the old snapshot can still reach a branch removed mid-flight; documented as the guarantee ("takes effect for items that enter the fork afterwards").
- Full-suite exit code is non-zero until issue #238 is fixed; the gate checks that only those 4 tests fail.
## Progress
- 2026-11-20 Task 1: red — DynamicBranchingTests on the dictionary fork → "Failed: 1, Passed: 12" (AddBranch_FromInsideRunningBranch_TakesEffectForLaterItems; failure message not captured).
- 2026-11-20 Ruling: `Stream` keeps its `branchOperators` constructor parameter but no longer stores it; `GetBranches()` reads the operator graph — avoids changing the public `SinkBuilder` constructor — cost if wrong: one unused internal parameter.
- 2026-11-20 Ruling: runtime branch names must be unique across the whole stream (checked under a stream-level lock), and `RemoveBranch` removes the name from every fork — `GetBranches()` is keyed by name — cost if wrong: two forks can no longer hold the same runtime name.
- 2026-11-20 Ruling: Task 3's "telemetry initialised once per downstream operator" test becomes "operators after the merge count each merged item once" — operators already re-propagate telemetry to their next operator (MapOperator.cs:59-63), so providers are set more than once today — cost if wrong: none; metrics are what users observe.
- 2026-11-20 Task 1: complete — build and filtered tests green (see commit feat(streams): add runtime AddBranch and RemoveBranch with thread-safe fork).
