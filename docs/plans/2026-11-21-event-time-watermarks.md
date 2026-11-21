# Plan: Event-time watermarks for Cortex.Streams (#197)
Status: in-progress
Date: 2026-11-21 · Agent: lead-developer · Size: large · Base: v3.2/release@1c6fc24b · Branch: v3/feature/197
Approved: 2026-11-21 via info.request
Consent: Full team (2026-11-21)
Execution: inline
Request: "In the continuation of enhancing Cortex v3.2, lets start planning and implementing the feature documented at github issue #197"

## Goal
A stream that calls `WithWatermarks(...)` windows and joins by event time (out-of-order events within the bound land in the right window, late events are dropped and reported, idle sources still close windows, the watermark survives a restart through a state store), while every stream without it behaves exactly as today, and a PR into `v3.2/release` closes #197.

## Context
- Issue #197 acceptance criteria: event-time windowing; out-of-order events within the threshold assigned correctly; late events handled via configuration; watermarks propagate through the operator chain; idle sources emit watermarks periodically; backward compatible; overhead <5% for in-order streams. Technical notes: min watermark across inputs, state cleanup after watermark + allowed lateness, min across sources for joins, watermark state in checkpoints.
- All six window operators assign windows by the event timestamp but close them on a 100 ms timer against `DateTime.UtcNow` (`src/Cortex.Streams/Operators/Windows/TumblingWindowOperator.cs:57,157-201`; Sliding `:67,194-235`; Session `:55,177-215`; AdvancedTumbling `:75,197-220`; AdvancedSliding `:84,233-256`; AdvancedSession `:60,271-304`). Basic operators recreate a closed window when a later event arrives; advanced fixed windows drop when `UtcNow > end + AllowedLateness` (`AdvancedTumblingWindowOperator.cs:153`); advanced session ignores lateness.
- `EventTimeTrigger` fires on processing time (`Operators/Windows/Triggers/EventTimeTrigger.cs:22-28`); `IWindowTrigger` has `OnElement`/`OnProcessingTime`/`Clear` only (`Triggers/WindowTrigger.cs`); `TriggerContext.CurrentProcessingTime` reads `UtcNow` (`Triggers/TriggerContext.cs:12-54`).
- `IOperator` is `Process(object)` + `SetNext` (`Operators/IOperator.cs:12,18`). Map, Filter, FlatMap, Aggregate, GroupByKey and Sink cast their input (`MapOperator.cs:68`, `FilterOperator.cs:85`, `FlatMapOperator.cs:111`, `SinkOperator.cs:65`), so an in-band watermark object would break them.
- `Stream` walks every operator once with a visited set to push telemetry, error handling and the stop action (`src/Cortex.Streams/Stream.cs:64-127`). Merge branches all end in one `MergeOperator` with no branch identity (`MergeOperator.cs:71-100`). `IStreamBuilder.SetNext(IOperator)` adds a custom operator (`StreamBuilder.cs:489-503`).
- Stream-stream join: one operator, right side fed by user code via `ProcessRight` (`StreamBuilder.cs:628-646`); matches `opposite.ExpiresAt > UtcNow` (`StreamStreamJoinOperator.cs:237-241,293-297`); cleanup `ExpiresAt <= UtcNow - GracePeriod` on a timer (`:118-123,345-350`). `StreamJoinConfiguration` has `WindowSize`, `JoinType`, `CleanupInterval`, `GracePeriod`, `MaxBufferSizePerKey` (`Operators/Joins/StreamJoinTypes.cs:119-179`).
- No generic checkpoint mechanism: only `IDataStore<K,V>` and `IStatefulOperator.GetStateStores()` (`src/Cortex.States/Abstractions/IDataStore.cs:5-17`, `src/Cortex.States/Operators/IStatefulOperator.cs:7`).
- No benchmark project or BenchmarkDotNet; precedent for a manual console tool: `tools/Cortex.Mediator.SourceGenerator.Smoke/`.
- Library: C# 8, targets net10.0..net7.0 + netstandard2.1 (no `TimeProvider` on netstandard2.1); tests: xUnit, net8.0, no `InternalsVisibleTo`, so tests use public API only.
- Flink: bounded out-of-orderness watermark trails the max timestamp by a fixed delay; downstream watermark is the min over inputs; idle inputs can be excluded (https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/). Kafka Streams: a record is dropped when stream time > window end + grace (https://kafka.apache.org/44/streams/core-concepts/).
- User decisions (2026-11-21, info.request): stream-level opt-in; idle sources advance from where they stopped at wall-clock speed; watermark persisted in an optional state store; overhead proven by a console bench in tools/.
- Design rubric (of 14): requirements 0-3, fits patterns and reuse 0-3, blast radius 0-3, testability 0-3, simplicity 0-2. Scores: minimal 12, pragmatic 11, clean 9.
- Assumption: event timestamps are UTC `DateTime`s, as the join docs already require.

## Approach
Watermarks travel out-of-band. `WithWatermarks(timestampSelector, WatermarkStrategy)` inserts a pass-through `WatermarkOperator<T>`: for each item it forwards first, then raises the max timestamp; when the watermark `W = maxTs - maxOutOfOrderness` advances, it calls `OnWatermark(W)` on every `IWatermarkAware` operator downstream of it, in topological order (so an upstream window's results reach a downstream window before that window sees the same watermark). `Stream` builds that subscriber list at construction (and again after runtime `AddBranch`/`RemoveBranch`) and calls `EnableEventTime(context)` on each subscriber; operators that don't implement the interface (Map, Filter, Sink, user operators) are untouched and transparent. Only one `WatermarkOperator` per path (a second one downstream throws at build).
In event-time mode a window's clock is its last received watermark: a window `[start, end)` fires when `W >= end`, its state is kept until `W >= end + AllowedLateness` (an allowed-late event re-fires it), an event whose window cleanup point is already `<= W` is late: dropped, counted and passed to `OnLateEvent`. The wall-clock timer is disposed; advanced triggers receive the watermark as their time (EventTimeTrigger, CountTrigger end-purge, and processing/early intervals all measure event time). Sessions keep several open sessions per key, extend or merge them for out-of-order events, and close at `W >= lastActivity + gap`. The join tracks the right side's watermark from `ProcessRight` timestamps with the same strategy, uses `min(left, right)` (an idle side drops out), matches when `|tL - tR| < WindowSize`, and expires at `ts + WindowSize + GracePeriod <= W`.
Idle: with `WithIdleTimeout(t)`, after `t` without events the watermark advances from where it stopped at wall-clock speed (`W = maxTs - delay + (now - lastEventAt - t)`); the operator's timer starts at the first event and pauses on `Stream.Stop`. Persistence: `WithStateStore(IDataStore<string,long>)` restores max timestamp and watermark at construction and saves them at most once per second while advancing and on stop.
- Rejected: in-band `Watermark` element — breaks every typed cast listed above.
- Rejected: a per-stream shared tracker that windows poll (minimal design) — firing would depend on a 100 ms timer again and tests would sleep; push notification keeps firing synchronous and deterministic.
- Rejected: per-window opt-in overloads and producer ids on Merge (clean design) — user chose stream-level opt-in; all merge branches share one source watermark, so per-branch minimums add nothing.
- Rejected: a new `IWindowTrigger` method — breaking for external triggers; the clock swap gives triggers event time without an interface change.
- Rejected: BenchmarkDotNet — new dependency; user chose a console bench.

## Out of scope
A coordinated checkpoint/restore of window and join buffers (no framework exists; only the watermark is persisted); watermark alignment; per-partition watermarks inside connectors; late-data side-output streams; stream-table joins (no time semantics); fixing the pre-existing CS0219 warnings; CI changes.

## Verification
- `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release` → exit 0, `0 Error(s)` on all five targets
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~Watermark|FullyQualifiedName~EventTime"` → exit 0, `Failed: 0`
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release` → exit 0, `Failed: 0`
- `dotnet run -c Release --project tools/Cortex.Streams.WatermarkBench` → exit 0, prints the overhead below 5%
- `git diff --stat v3.2/release...HEAD -- src/Cortex.Tests` → only new files (existing tests unchanged = backward compatible)
- End to end: `EventTimeEndToEndTests` builds Source → WithWatermarks → Map → TumblingWindow → Sink with shuffled events inside the bound, one late event and an idle tail, and asserts exact window contents, one `LateEvent`, and that the last window closes through idleness.

## Orchestration
- Consent: Full team (2026-11-21) · Estimate: 16 agents, 8 waves, about 66 cards (8 extra), ≈4.5× Solo tokens
- Companions: researcher=researcher · software-architect=software-architect · developer=developer · code-reviewer=code-reviewer · security-reviewer=— · qa-engineer=qa-engineer · debugger=debugger · ui-ux-designer=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Understand | researcher × 4 | read-only | windows; operator chain; joins/checkpoints; tests + Flink/Kafka | claims cited, key lines re-read | done |
| 2 | Design | software-architect × 3 | read-only | minimal; clean; pragmatic | rubric of 14 | done |
| 3 | Implement | lead inline | write | Tasks 1-9 (shared contract, coupled files) | verify-and-commit per task | pending |
| 4 | Review | code-reviewer × 4 (stand-ins if refused) | read-only | correctness; spec; tests and silent failures; simplify | THRESHOLD 80 | pending |
| 5 | Refute | code-reviewer × 3 | read-only | reachability; impact; defenses | ≥2 of 3 | pending |
| 6 | Fix | lead inline | write | confirmed findings | red then green | pending |
| 7 | Re-review | code-reviewer × 1 | read-only | fix commits | ALL_ADDRESSED | pending |
| 8 | Critic | code-reviewer × 1 | read-only | completeness vs request and #197 | COMPLETE | pending |

## Tasks
### Task 1: Add watermark core, WithWatermarks and stream wiring
- [x] Done
Files: `src/Cortex.Streams/Watermarks/WatermarkStrategy.cs` (create), `src/Cortex.Streams/Watermarks/LateEvent.cs` (create), `src/Cortex.Streams/Watermarks/IWatermarkAware.cs` (create), `src/Cortex.Streams/Watermarks/EventTimeContext.cs` (create), `src/Cortex.Streams/Watermarks/WatermarkOperator.cs` (create), `src/Cortex.Streams/Abstractions/IStreamBuilder.cs` (modify), `src/Cortex.Streams/StreamBuilder.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Tests/Streams/Tests/WatermarkTests.cs` (create)
Depends on: none
- [x] Write `WatermarkTests` first: `W = maxTs - delay`; monotonic (an older event never lowers it); `ForMonotonousTimestamps` uses delay 0; subscribers receive `OnWatermark` only when W advances, after the item reached them; topological order across Fork → two branches → Merge → aware operator; `EnableEventTime` is called once with the strategy's lateness; a second `WithWatermarks` on the same path throws `InvalidOperationException`; a runtime `AddBranch` with an aware operator gets subscribed and `RemoveBranch` unsubscribes it; idle advance with a fake clock via `WithClock` + public `CheckIdle()` follows `maxTs - delay + (now - lastEventAt - t)`; a stream without `WithWatermarks` never calls an aware operator. Run them red (compile failure counts).
- [x] Implement the types and `IStreamBuilder.WithWatermarks(Func<TCurrent, DateTime>, WatermarkStrategy)`; `Stream` builds the subscriber list (reverse post-order over the same successor rule as `CollectOperators`), rebuilds it in `AddBranch`/`RemoveBranch`, pauses idle timers in `Stop`/`StopAsync`, resumes in `Start`, and disposes them in `Dispose`.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~WatermarkTests|FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): add watermark strategy and event-time wiring

### Task 2: Event-time mode for tumbling and sliding windows
- [ ] Done
Files: `src/Cortex.Streams/Operators/Windows/TumblingWindowOperator.cs` (modify), `src/Cortex.Streams/Operators/Windows/SlidingWindowOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/EventTimeWindowTests.cs` (create)
Depends on: Task 1
- [ ] Tests first (no sleeps; drive `Process` + `OnWatermark` through a built stream or directly): in-order and shuffled-within-bound events land in exact windows; a window fires once at `W >= end`; an event for a window whose `end + lateness <= W` is dropped and reported as `LateEvent` (not a recreated window); within allowed lateness the window re-fires with all items; state is removed at `W >= end + lateness`; sliding checks each window separately; the wall-clock timer no longer fires in event-time mode.
- [ ] Implement `IWatermarkAware` with a `_nextDeadline` fast path so an `OnWatermark` that changes nothing takes no lock.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeWindowTests|FullyQualifiedName~TumblingWindowOperatorTests|FullyQualifiedName~SlidingWindowOperatorTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): event-time tumbling and sliding windows

### Task 3: Event-time mode for session windows
- [ ] Done
Files: `src/Cortex.Streams/Operators/Windows/SessionWindowOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/EventTimeSessionWindowTests.cs` (create)
Depends on: Task 1
- [ ] Tests first: an out-of-order event inside the gap extends the session (start = min, last = max); an event bridging two open sessions merges them; a far-ahead event opens a second session for the same key instead of closing the first; a session closes at `W >= last + gap`; a late event is dropped and reported; processing-time sessions unchanged.
- [ ] Implement several open sessions per key in event-time mode (store keys per session plus an in-memory index), closing on watermark only.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeSessionWindowTests|FullyQualifiedName~SessionWindowOperatorTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): event-time session windows

### Task 4: Event-time mode for advanced tumbling and sliding windows
- [ ] Done
Files: `src/Cortex.Streams/Operators/Windows/Triggers/TriggerContext.cs` (modify), `src/Cortex.Streams/Operators/Windows/AdvancedTumblingWindowOperator.cs` (modify), `src/Cortex.Streams/Operators/Windows/AdvancedSlidingWindowOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/EventTimeAdvancedWindowTests.cs` (create)
Depends on: Task 2
- [ ] Tests first: default `EventTimeTrigger` fires at `W >= end` (not on wall clock, shown with year-2020 timestamps); `CountTrigger` fires on count and purges at `W >= end`; `WindowConfiguration.AllowedLateness`/`OnLateEvent` take precedence over the strategy's when set; an allowed-late event re-fires with emission type Late; state removed at `W >= end + lateness`.
- [ ] Give `TriggerContext` a time source (wall clock by default, last watermark in event-time mode) and evaluate triggers on `OnWatermark` with the watermark as their time; dispose the wall-clock timer.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeAdvancedWindowTests|FullyQualifiedName~AdvancedWindowingTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): event-time advanced tumbling and sliding windows

### Task 5: Event-time mode for advanced session windows
- [ ] Done
Files: `src/Cortex.Streams/Operators/Windows/AdvancedSessionWindowOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/EventTimeAdvancedWindowTests.cs` (modify)
Depends on: Task 3, Task 4
- [ ] Tests first: out-of-order extend and merge as in Task 3; the session honours `AllowedLateness` and `OnLateEvent`; triggers run on watermark time.
- [ ] Implement, reusing Task 3's session index approach.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeAdvancedWindowTests|FullyQualifiedName~AdvancedWindowingTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): event-time advanced session windows

### Task 6: Event-time mode for stream-stream joins
- [ ] Done
Files: `src/Cortex.Streams/Operators/Joins/StreamStreamJoinOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/EventTimeJoinTests.cs` (create)
Depends on: Task 1
- [ ] Tests first: replayed old events still match when `|tL - tR| < WindowSize` and don't match outside it; nothing expires until both sides have advanced (min); with an idle timeout an idle right side stops holding the left back; unmatched left/outer results are emitted at `ts + WindowSize + GracePeriod <= W`; a late element is reported and not buffered; processing-time joins unchanged.
- [ ] Implement right-side tracking from `ProcessRight` timestamps with the stream's strategy, combined watermark, watermark-driven cleanup, and a timer used only for idle checks in event-time mode.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeJoinTests|FullyQualifiedName~StreamStreamJoinOperatorTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): event-time stream-stream joins

### Task 7: Persist watermark state in a state store
- [ ] Done
Files: `src/Cortex.Streams/Watermarks/WatermarkStrategy.cs` (modify), `src/Cortex.Streams/Watermarks/WatermarkOperator.cs` (modify), `src/Cortex.Tests/Streams/Tests/WatermarkPersistenceTests.cs` (create)
Depends on: Task 1
- [ ] Tests first: with `WithStateStore`, a rebuilt stream on the same store resumes at the saved watermark (an event older than it is late); saves are throttled to once per second while advancing and flushed on `Stop`; the store appears in `IStream.GetStateStores()`; without a store nothing is written.
- [ ] Implement `IStatefulOperator` on `WatermarkOperator`.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~WatermarkPersistenceTests|FullyQualifiedName~WatermarkTests"`
Expected: exit 0, `Failed: 0`
Commit: feat(streams): persist watermarks in a state store

### Task 8: End-to-end test and overhead bench
- [ ] Done
Files: `src/Cortex.Tests/Streams/Tests/EventTimeEndToEndTests.cs` (create), `tools/Cortex.Streams.WatermarkBench/Cortex.Streams.WatermarkBench.csproj` (create), `tools/Cortex.Streams.WatermarkBench/Program.cs` (create)
Depends on: Tasks 2-7
- [ ] Write the end-to-end test from Verification.
- [ ] Bench: 2M in-order items through Map → Filter → Sink with and without `WithWatermarks`, warm-up, median of 7 runs each; print both medians and the overhead; exit 1 if overhead ≥ 5%. Also print (not gate) a TumblingWindow processing-time vs event-time comparison.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~EventTimeEndToEndTests" && dotnet run -c Release --project tools/Cortex.Streams.WatermarkBench`
Expected: exit 0, `Failed: 0`, bench prints overhead < 5%
Commit: test(streams): add event-time end-to-end test and watermark bench

### Task 9: Document event time and watermarks
- [ ] Done
Files: `docs/wiki/Event-Time-and-Watermarks.md` (create), `docs/wiki/Branch-and-Merge.md` (modify), `docs/wiki/Stream-Stream-Joins.md` (modify)
Depends on: Tasks 1-8
- [ ] New page with self-contained, realistic examples (out-of-order IoT readings, Kafka replay with idle timeout, late-event callback, persisted watermark, event-time join), the firing/lateness rules and limits; link it from the two pages where they describe wall-clock closing (`Branch-and-Merge.md:341`, `Stream-Stream-Joins.md:26-40`). Each full example compiled and run in a throwaway test, deleted before commit.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: docs(streams): document event time and watermarks

## Risks
- Adding `WithWatermarks` to `IStreamBuilder` breaks external implementers of that interface (the #155 PR already added members); called out in the PR.
- Idle advance assumes event time moves roughly with the wall clock; events arriving after an idle advance can be late. Opt-in and documented.
- With several buffered consumers, items reach operators out of order; the out-of-orderness bound must cover it. Documented.
- Processing-time and early-trigger intervals become event-time intervals under watermarks — a semantic change only for streams that opt in. Documented.
- Window emission still happens under the operator lock (as today); `OnLateEvent` runs on the processing thread.
- The bench is noisy on a busy machine; median of 7 and a pure-tracking pipeline keep it stable.

## Progress
- 2026-11-21 Task 1: red — `dotnet test ... --filter "FullyQualifiedName~WatermarkTests"` → CS1061 'WithWatermarks' not found, CS0246 'WatermarkOperator<>' not found
- 2026-11-21 Task 1: complete — `dotnet test ... --filter "FullyQualifiedName~WatermarkTests|FullyQualifiedName~DynamicBranchingTests|FullyQualifiedName~BranchMergeTests"` → Passed! - Failed: 0, Passed: 48
- 2026-11-21 Ruling: the runtime AddBranch/RemoveBranch event-time tests move to Task 2 — branch builders have no SetNext, so they need a real window — cost if wrong: Task 1 commit ships that wiring tested one commit later.
- 2026-11-21 Ruling: branch `v3/feature/197` instead of `sophon/<slug>` — repo naming `v3/feature/<issue>` — cost if wrong: a branch rename.
- 2026-11-21 Ruling: hybrid design — pragmatic push wiring with the minimal design's single source watermark per stream and stream-level opt-in — the user's choices — cost if wrong: per-window time modes need a later overload.
- 2026-11-21 Ruling: implementation inline, no developer agents — every task shares the new contract and its files — cost if wrong: none beyond tokens.
- 2026-11-21 Ruling: the bench gates on pure watermark-tracking overhead (Map → Filter → Sink); the windowed comparison is informational — event-time windows emit results the processing-time run doesn't, so their timings aren't comparable — cost if wrong: the window path's overhead is reported, not gated.
