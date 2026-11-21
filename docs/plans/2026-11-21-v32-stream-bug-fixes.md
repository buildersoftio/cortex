# Plan: Fix Cortex.Streams v3.2 bugs #238, #241-#246
Status: in-progress
Date: 2026-11-21 · Agent: lead-developer · Size: large · Base: v3.2/release@71768f4 · Branch: v3/bug/238-246
Approved: 2026-11-21 via info.request
Consent: Specialist team (2026-11-21)
Execution: inline
Request: "We have created 7 bug issues in github, that we want to fix them and merge with v3.2/release branch, lets analyze them and implement the fix."

## Goal
All seven v3.2 bug issues (#238, #241, #242, #243, #244, #245, #246) are fixed with regression tests on one branch, and a PR into `v3.2/release` closes them.

## Context
- Baseline (run 2026-11-21): `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~SourceOperatorErrorHandlingTests|FullyQualifiedName~SessionWindowOperatorTests"` → exit 1, `Failed: 4, Passed: 16`.
- #238 retry: every `ErrorHandlingHelper.TryExecute` overload catches `Exception`, including the terminal `StreamStoppedException` thrown by a nested helper (`src/Cortex.Streams/ErrorHandling/ErrorHandlingHelper.cs:35,89,146`). The source adapter wraps the whole downstream call in a helper (`src/Cortex.Streams/Operators/SourceOperatorAdapter.cs:154-158`), so the Map's "max retries exceeded" stop is retried again: 2 × 2 = 4 calls. The test's expected 2 matches the documented attempt semantics (`src/Cortex.Tests/Streams/Tests/ErrorHandlingTests.cs:198-232`).
- #238 skip: the assertion runs after `stream.Stop()`, which sets `WasStoppedExplicitly` (`SourceOperatorErrorHandlingTests.cs:165-170`).
- #238 session: fixed `Thread.Sleep` on a strict expiry boundary (`SessionWindowOperatorTests.cs:33-43,126-143`); the repo already uses bounded `SpinWait.SpinUntil` (`BranchMergeTests.cs:383`).
- #241/#242: `BufferedProcessor` maps `DropNewest` to the channel's buffered-item eviction and creates the channel without a drop callback (`src/Cortex.Streams/Performance/BufferedProcessor.cs:65-78`); closed `Block` writes call `OnItemDropped` without counting (`:196-199,223-226`); a closed drop-mode write is labelled buffer-full (`:250-256`). `Channel.CreateBounded<T>(options, Action<T> itemDropped)` exists in-box on net7+ and in System.Threading.Channels 9.0.0 for netstandard2.1 (`src/Cortex.Streams/Cortex.Streams.csproj:55`).
- #243: `CompleteAsync` builds a 30 s linked token but awaits `Task.WhenAll(_consumerTasks)` without it (`BufferedProcessor.cs:359-374`). `AllowSynchronousContinuations = true` (`:75`) lets a write or `Complete()` run consumer code inline on the caller's thread.
- #244: both consumers wrap `_processAction` in a bare `catch { }` (`BufferedProcessor.cs:265-274,327-335`).
- #246: `Start()` keeps a completed processor because `EnsureBufferedProcessorInitialized` returns when `_bufferedProcessor != null` (`src/Cortex.Streams/Stream.cs:124-139,157-166`); the processor's stop callback sets `_isStarted = false` (`:137`).
- #245: `CleanupExpiredElements` runs the join function and `_nextOperator.Process` under `_bufferLock` with no error handling, on a `System.Threading.Timer` (`src/Cortex.Streams/Operators/Joins/StreamStreamJoinOperator.cs:116-120,319-412`); arrival paths use `ErrorHandlingHelper.TryExecute` (`:155-163,186-194`). `ForceCleanup()` calls the same method synchronously (`:417-420`).
- Library targets net10.0..net7.0 + netstandard2.1, `LangVersion 8.0`; tests target net8.0.
- Assumption: per-run buffer statistics after a restart match `IStream.cs:138-150` ("since the stream started").

## Approach
Fix each issue at its root, one task per issue group, all in `Cortex.Streams`, each with tests written to fail first.
1. `TryExecute` rethrows `StreamStoppedException` untouched in all three overloads, so a terminal stop is never retried or re-decided by an outer operator.
2. `DropOldest` uses the channel's `itemDropped` callback to count and report the evicted item. `DropNewest` uses `Wait` mode with a non-blocking `TryWrite`: when full, the incoming item is counted, reported as `BufferFullDropNewest`, and `EmitAndForget` returns `false`. A `_writerCompleted` flag tells "full" from "stopped"; every stopped rejection is counted and reported as `StreamStopped`.
3. `CompleteAsync` races the consumers against the caller's token and the 30 s timeout. On cancellation it cancels the consumers (they stop after the current item) and throws `OperationCanceledException`; on timeout it cancels them and returns. `AllowSynchronousContinuations` becomes `false`, so emits and stops never run the pipeline inline.
4. Escaped pipeline exceptions are counted in a new `BufferStatistics.TotalFailed` and passed to a new `StreamPerformanceOptions.OnProcessingError(object item, Exception ex)` callback; an exception from the callback itself is caught so the consumer keeps running.
5. `Start()` after a stop creates a fresh processor (new buffer, consumers, statistics); the old processor's stop callback only affects the run it belongs to.
6. Join expiry collects expired elements and updates buffers and stores under the lock, then emits unmatched results outside it, each through `TryExecute` with the stream's options. The timer callback catches everything: a `StreamStoppedException` stops the owning stream (through a new internal hook set by `Stream`), anything else is written with `Trace.TraceError` and the next element continues. `ForceCleanup()` keeps throwing to its caller.
- Rejected: `BoundedChannelFullMode.DropWrite` for #242 — `TryWrite` still returns `true`, so callers can't see the rejection.
- Rejected: rejecting restart with `InvalidOperationException` for #246 — smaller, but every host that restarts a stream would have to rebuild it.
- Rejected: applying `WithErrorHandling` in the consumer for #244 — operators already apply it, so `OnError` would run twice for the same failure.
- Rejected: changing `MaxRetries` semantics for #238 — the existing tests define it as the attempt limit.

## Out of scope
Renaming `MaxRetries`; disposal races between `StreamStreamJoinOperator.Dispose` and a running timer callback; `EmitAsync`/`EmitBatchAsync` still return `Task` (no acceptance result); the pre-existing CS0219 warnings.

## Verification
- `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release` → exit 0, `0 Error(s)` on all five targets
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~Cortex.Streams.Tests"` → exit 0, `Failed: 0`
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release` → exit 0, `Failed: 0`
- End to end: the reproductions from each issue body exist as tests and pass; the four #238 tests pass in three consecutive focused runs.

## Orchestration
- Consent: Specialist team (2026-11-21) · Estimate: 17 agents, 8 waves, about 145 cards (23 extra), ≈8× Solo tokens
- Companions: researcher=researcher · software-architect=software-architect · developer=developer · code-reviewer=code-reviewer · security-reviewer=— · qa-engineer=qa-engineer · debugger=debugger · ui-ux-designer=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Understand | researcher × 3 | read-only | buffer/lifecycle; join timer; #238 tests | claims cited, key lines re-read | done |
| 2 | Design | software-architect × 2 | read-only | minimal; pragmatic | rubric | done (both returned empty; designed inline) |
| 3 | Implement | lead inline | write | Tasks 1-6 (coupled files) | verify-and-commit per task | pending |
| 4 | Review | code-reviewer × 3, qa-engineer × 1 | read-only | correctness; spec; concurrency; test gaps | THRESHOLD 80 | pending |
| 5 | Refute | code-reviewer × 3 | read-only | reachability; impact; defenses | ≥2 of 3 | pending |
| 6 | Fix | lead inline | write | confirmed findings | red then green | pending |
| 7 | Re-review | code-reviewer × 1 | read-only | fix commits | ALL_ADDRESSED | pending |
| 8 | Critic | code-reviewer × 1 | read-only | completeness | COMPLETE | pending |

## Tasks
### Task 1: Stop retrying terminal stops and fix the four #238 tests
- [x] Done
Files: `src/Cortex.Streams/ErrorHandling/ErrorHandlingHelper.cs` (modify), `src/Cortex.Tests/Streams/Tests/SourceOperatorErrorHandlingTests.cs` (modify), `src/Cortex.Tests/Streams/Tests/SessionWindowOperatorTests.cs` (modify), `src/Cortex.Tests/Streams/Tests/ErrorHandlingTests.cs` (modify)
Depends on: none
- [x] Add `catch (StreamStoppedException) { throw; }` before the generic catch in all three `TryExecute` overloads.
- [x] Add a test: a nested Map with Retry inside a stream with Retry runs the map exactly `MaxRetries` times and stops; and one where `OnError` is not called for the inner stop.
- [x] Skip test: assert `IsRunning` and `!WasStoppedExplicitly` before `stream.Stop()`, and `WasStoppedExplicitly` after it.
- [x] Session tests: replace the fixed sleeps with `SpinWait.SpinUntil` on a locked count (10 s cap), read the results from a locked snapshot, dispose in `finally`; keep every value assertion.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~SourceOperatorErrorHandlingTests|FullyQualifiedName~SessionWindowOperatorTests|FullyQualifiedName~Cortex.Streams.Tests.ErrorHandlingTests"`
Expected: exit 0, `Failed: 0`
Commit: fix(streams): stop retrying terminal stops and stabilise #238 tests

### Task 2: Report every buffer drop and drop the incoming item under DropNewest
- [x] Done
Files: `src/Cortex.Streams/Performance/BufferedProcessor.cs` (modify), `src/Cortex.Streams/Performance/BackpressureStrategy.cs` (modify), `src/Cortex.Tests/Streams/Tests/StreamPerformanceTests.cs` (modify), `docs/wiki/stream-performance.md` (modify)
Depends on: Task 1
- [x] Tests (red first): DropOldest with capacity 2 and a blocked sink → processed `[1,3,4]`, one `BufferFullDropOldest` callback for item 2, `TotalDropped == 1`; DropNewest → processed `[1,2,3]`, `EmitAndForget(4) == false`, one `BufferFullDropNewest` callback for 4, `TotalDropped == 1`; a closed-channel write under Block and under DropOldest → `StreamStopped`, counted.
- [x] Create the channel with an `itemDropped` callback for DropOldest; use `Wait` mode + `TryWrite` for DropNewest; add `_writerCompleted`; count every stopped rejection.
- [x] Replace the existing DropNewest test that expects every write to succeed; update the enum XML docs and the wiki (remove "Known Limitation: Drop Callbacks and Counters", fix the DropNewest and `TotalDropped` text).
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~StreamPerformanceTests"`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: fix(streams): report buffer drops and drop incoming items on DropNewest

### Task 3: Make StopAsync honour its token and the shutdown timeout
- [x] Done
Files: `src/Cortex.Streams/Performance/BufferedProcessor.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Streams/Abstractions/IStream.cs` (modify), `src/Cortex.Tests/Streams/Tests/StreamPerformanceTests.cs` (modify), `docs/wiki/stream-performance.md` (modify)
Depends on: Task 2
- [x] Tests (red first): blocked sink + already-cancelled token → `StopAsync` throws `OperationCanceledException` within 2 s while the sink is still blocked; a token cancelled after 200 ms → same; repeated `StopAsync` does not throw.
- [x] Race `Task.WhenAll(consumers)` against `Task.Delay(Infinite, timeoutCts.Token)`; on cancel or timeout cancel `_cts`; throw for the caller's token, return on timeout; use `TryComplete`; set `AllowSynchronousContinuations = false`.
- [x] Update the `StopAsync` XML docs and the wiki "Graceful Shutdown" section.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~StreamPerformanceTests"`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: fix(streams): honour cancellation and timeout in StopAsync

### Task 4: Report pipeline failures in buffered processing
- [x] Done
Files: `src/Cortex.Streams/Performance/BufferedProcessor.cs` (modify), `src/Cortex.Streams/Performance/StreamPerformanceOptions.cs` (modify), `src/Cortex.Streams/Abstractions/IStream.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Tests/Streams/Tests/StreamPerformanceTests.cs` (modify), `docs/wiki/stream-performance.md` (modify)
Depends on: Task 3
- [x] Tests (red first, single and batch consumers): Map throws on 2 → sink `[1,3]`, `OnProcessingError` called once with item 2 and the exception, `TotalFailed == 1`, `TotalProcessed == 2`; a throwing callback does not stop item 3.
- [x] Add `OnProcessingError`, `BufferStatistics.TotalFailed`, a shared `ProcessOne(item)` used by both consumers.
- [x] Update the wiki (best practice 6, options table, statistics table).
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~StreamPerformanceTests"`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: fix(streams): report pipeline failures in buffered processing

### Task 5: Restart buffered streams with a fresh buffer
- [x] Done
Files: `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Streams/Performance/BufferedProcessor.cs` (modify), `src/Cortex.Tests/Streams/Tests/StreamPerformanceTests.cs` (modify), `docs/wiki/stream-performance.md` (modify)
Depends on: Task 4
- [x] Tests (red first): issue #246 reproduction after `StopAsync` and after `Stop` → sink `[1,2,3]`, `EmitAndForget(2) == true`, no drop callbacks, `RUNNING`; statistics reset for the new run; `Start()` twice while running keeps the same buffer.
- [x] Add `IsCompleted` to the processor; `Start()` replaces a completed processor under `_processorLock`; the stop callback captures its processor and only clears `_isStarted` if it is still current.
- [x] Document restart in the wiki.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~StreamPerformanceTests"`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: fix(streams): restart buffered streams with a fresh buffer

### Task 6: Handle errors in stream-join expiry and emit outside the lock
- [x] Done
Files: `src/Cortex.Streams/Operators/Joins/StreamStreamJoinOperator.cs` (modify), `src/Cortex.Streams/Stream.cs` (modify), `src/Cortex.Streams/Operators/IStreamStopAware.cs` (create), `src/Cortex.Tests/Streams/Tests/StreamStreamJoinOperatorTests.cs` (modify), `docs/wiki/Stream-Stream-Joins.md` (modify)
Depends on: Task 5
- [x] Tests (red first with `ForceCleanup`): Skip strategy + throwing join function for one unmatched element → no exception, the other unmatched element is still emitted, buffers empty; a downstream sink that reads `GetLeftBufferCount()` from another thread completes (no lock held). Then timer tests (CleanupInterval 50 ms): default options + throwing join function → process survives and the next element is emitted; Stop strategy → the stream's `GetStatus()` becomes `NOT_RUNNING`.
- [x] Split expiry into a locked collect/remove/store step and an unlocked emit step using `TryExecute`; add the timer-boundary callback; add internal `IStreamStopAware` and set it from `Stream.InitializeOperators`.
- [x] Update the wiki sections at `Stream-Stream-Joins.md:40,305,597-613`.
Run: `dotnet build src/Cortex.Streams/Cortex.Streams.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release --filter "FullyQualifiedName~StreamStreamJoin"`
Expected: exit 0, `0 Error(s)`, `Failed: 0`
Commit: fix(streams): handle errors when join expiry emits unmatched events

## Risks
- `AllowSynchronousContinuations = false` adds a thread hop per item in buffered mode (small throughput cost).
- New public members (`OnProcessingError`, `TotalFailed`) are additive; `DropNewest` now rejects instead of evicting, which is a visible behaviour change matching its documented contract.
- Timer-based tests can be slow under load; each uses a bounded condition wait, never a fixed sleep.

## Progress
- 2026-11-21 Ruling: branch `v3/bug/238-246` instead of `sophon/<slug>` — matches the repo's `v3/bug/<issue>` naming — cost if wrong: a branch rename.
- 2026-11-21 Task 1: red — `SourceOperatorErrorHandlingTests` → `Failed: 2, Passed: 8` (retry count, new OnError test) before the helper fix.
- 2026-11-21 Task 1: complete — Run line × 3 → `Passed! - Failed: 0, Passed: 53` each time.
- 2026-11-21 Ruling: the existing retry test (expects 2 calls) was right; the nested-retry fix in `ErrorHandlingHelper` is the root cause, and the test itself is the regression test — cost if wrong: none.
- 2026-11-21 Task 2: red — `StreamPerformanceTests` → `Failed: 4, Passed: 16` (the 3 new drop tests + the rewritten DropNewest test).
- 2026-11-21 Task 2: complete — build `0 Error(s)`; `StreamPerformanceTests` → `Passed! - Failed: 0, Passed: 20`.
- 2026-11-21 Ruling: no public-API test for closed-channel writes — `Emit*` throw before reaching the buffer once `_isStarted` is false, so a closed write needs a stop/emit race; the counting is a one-line `ReportDrop` call on each path — cost if wrong: an uncounted stopped drop goes unnoticed. Also: `ThrowException` on a closed buffer now reports `StreamStopped` and returns `false` instead of a misleading `BufferFullException`.
- 2026-11-21 Task 3: red — `StreamPerformanceTests` → `Failed: 3, Passed: 20` (both cancellation tests, double `StopAsync`).
- 2026-11-21 Task 3: complete — build `0 Error(s)`; `StreamPerformanceTests` → `Passed! - Failed: 0, Passed: 23`.
- 2026-11-21 Ruling: when `StopAsync` gives up (token or timeout) and when `Stop()` runs, items still buffered are reported as `StreamStopped` drops, and consumers check for cancellation before taking the next item (replacing `ReadAllAsync`, which kept yielding buffered items after cancellation) — completes #241's "every drop is reported" — cost if wrong: `Stop()` now calls `OnItemDropped` for undrained items where it was silent.
- 2026-11-21 Cap: the 30 s timeout path has no test — the timeout is not configurable and a 30 s test is too slow; it shares the code path with the cancellation tests.
- 2026-11-21 Task 4: red — the new tests did not compile before `OnProcessingError`/`TotalFailed` existed (not run separately).
- 2026-11-21 Task 4: complete — build `0 Error(s)`; `StreamPerformanceTests` → `Passed! - Failed: 0, Passed: 26`.
- 2026-11-21 Task 5: red — `StreamPerformanceTests` → `Failed: 2, Passed: 27` (restart after `StopAsync` and after `Stop`).
- 2026-11-21 Task 5: complete — build `0 Error(s)`; `StreamPerformanceTests` → `Passed! - Failed: 0, Passed: 29`.
- 2026-11-21 Ruling: only `Start()` replaces a completed processor; emit methods never do — an emit racing a stop must not revive the buffer — cost if wrong: none. The old processor is not disposed on restart, because `Dispose` can block up to 5 s on a slow sink — cost if wrong: one undisposed `CancellationTokenSource` per restart.
- 2026-11-21 Task 6: complete — build `0 Error(s)`; `FullyQualifiedName~StreamStreamJoin` → `Passed! - Failed: 0, Passed: 23` (5 new).
- 2026-11-21 Task 6: red run not captured — the tests and the fix were written before a red run; the two timer tests would crash the test host on the old code, and the three `ForceCleanup` tests fail on it by construction (no error handling, emit under the lock). Weaker proof for this task.
- 2026-11-21 Ruling: unmatched counters now count a result only after it was emitted (on Skip it is not counted) — matches "unmatched results emitted" — cost if wrong: telemetry differs slightly from before.
- 2026-11-21 Review: qa-engineer gaps pass (1 agent); the 2 stand-in reviewers (correctness, spec) did not start ("Invalid agent ID"), so those lenses ran inline. 3 findings confirmed by reading the code and fixed: a batch consumer stopped while collecting lost its collected items silently (`BufferedProcessor.cs` outer catch); a throwing `OnItemDropped` could fault a consumer or break a stop; a stop decision from an item of an earlier run stopped the restarted stream (`Stream.ProcessItem`). Tests added: partial-batch stop, restart with an in-flight stop decision, callback invocation asserted, right-join expiry with a failing sink. `StreamPerformanceTests|StreamStreamJoin` → `Passed! - Failed: 0, Passed: 55`. Red runs for these fixes were not captured.
- 2026-11-21 Ruling: `OnItemDropped` exceptions are now swallowed everywhere (as `OnProcessingError`'s are) \u2014 the callback also runs on consumers and inside channel writes \u2014 cost if wrong: an emitter no longer sees an exception thrown by its own drop callback.
- 2026-11-21 Ruling: user chose callback + counter (#244), clean restart (#246), error handling on the join timer (#245).
