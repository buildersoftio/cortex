# Plan: Fix the eight Cortex.Mediator bugs #250-#257
Status: in-progress
Date: 2026-11-22 · Agent: lead-developer · Size: large · Base: v3.2/release@1bd8088 · Branch: v3/bug/250-257
Approved: 2026-11-22 via info.request
Consent: Review panel (2026-11-22)
Execution: inline
Request: "Yes lets fix the 8 bugs in one PR."

## Goal
Issues #250-#257 are fixed on one branch: each has a test that fails on 1bd8088 and passes after the fix, the Mediator test classes pass on net11.0 and net8.0, the Release build passes, and one PR goes into `v3.2/release`.

## Context
- Each issue body has the cause with path:line and repro steps; the six reproduced on 2026-11-22 (#250-#255) failed exactly as described with a temporary test.
- #250: `RegisterPipelineBehaviors` registers every command behavior against the open interfaces (`src/Cortex.Mediator/DependencyInjection/ServiceCollectionExtensions.cs:120-129`); query and notification behaviors already split open and closed types (`:132-171`).
- #251: assemblies are not de-duplicated (`ServiceCollectionExtensions.cs:43`, `:64`) and `services.Add` adds every descriptor (`:108`).
- #252: `CacheInvalidator.TrackKey` (`src/Cortex.Mediator/Caching/CacheInvalidator.cs:82-89`) has no callers; `CachingQueryBehavior` writes entries without telling the invalidator (`src/Cortex.Mediator/Behaviors/CachingQueryBehavior.cs:80-86`). `AddMediatorCaching` registers `ICacheInvalidator` as a singleton (`src/Cortex.Mediator/DependencyInjection/CachingServiceCollectionExtensions.cs:39`, `:73`).
- #253: keys use `queryType.Name` (`src/Cortex.Mediator/Caching/DefaultCacheKeyGenerator.cs:52`, `:59`, `:62`). Existing tests assert only `StartsWith(prefix)` and `Contains("TestQuery")` (`src/Cortex.Tests/Mediator/Tests/DefaultCacheKeyGeneratorTests.cs:50`, `:63`), which a full name still satisfies.
- #254: `CreateStream<TResult>` dispatches with `MethodInfo.Invoke` (`src/Cortex.Mediator/Mediator.cs:259`) to a non-async method, so synchronous errors arrive as `TargetInvocationException`. The other four `Invoke` calls (`:69`, `:104`, `:139`, `:209`) share the pattern.
- #255: the handler delegates call `handler.Handle` directly (`Mediator.cs:164`, `:175`); `ParallelNotificationStrategy` builds its task array with `Select(handler => handler()).ToArray()` (`src/Cortex.Mediator/Notifications/ParallelNotificationStrategy.cs:19`); the generated mediator emits the same delegates (`src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedMediatorEmitter.cs:285-305`). `GeneratedMediatorRuntimeTests` compiles and runs generated code, so the generated path can be tested at runtime.
- #256: the three FluentValidation behaviors block on `.Result` and call `ValidateAsync(context)` without the token (`ValidationCommandBehavior.cs:26-28`, `ValidationQueryBehavior.cs:25-27`, `VoidValidationCommandBehavior.cs:27-29`). FluentValidation 11.11.0 has `ValidateAsync(IValidationContext, CancellationToken)`.
- #257: `suppressExceptions` is a `bool` constructor parameter (`src/Cortex.Mediator/Behaviors/ExceptionHandlingNotificationBehavior.cs:31-38`) of a behavior the container builds (`MediatorOptionsExtensions.cs:31`), so it is always `false`; the README section is a placeholder (`src/Cortex.Mediator/README.md:246-251`). Unit tests construct the behavior by hand with that parameter (`src/Cortex.Tests/Mediator/Tests/ExceptionHandlingNotificationBehaviorTests.cs:79-216`), so the parameter must stay.
- `RegisterPipelineBehaviors` is public and the generated DI code calls it (`GeneratedDIEmitter.cs:92`), so changes there apply to both modes.
- AGENTS.md: "Code has to compile for every TFM in the list" (Cortex.Mediator targets net11.0 down to netstandard2.0; the FluentValidation package down to netstandard2.1); "Match the style of the surrounding code"; "Commits use Conventional Commits with a scope"; "Branches are named `v3/feature/<issue>` or `v3/bug/<issue>`, and PRs go into the current `v3.x/release` branch"; "The root `README.md` is the NuGet readme for most packages" (the Mediator family packs its own README).
- Assumption: Microsoft.Extensions.DependencyInjection fills an unresolvable optional constructor parameter with its default value, as the existing `IExceptionHandler? exceptionHandler = null` parameters already rely on.

## Approach
Fix each issue at its cause with the smallest change and a red-then-green test, one commit per issue, in file order so that edits to shared files (`ServiceCollectionExtensions.cs`, `Mediator.cs`) stay sequential.
- #250: register closed command behaviors against the closed `ICommandPipelineBehavior<,>` / `ICommandPipelineBehavior<>` interfaces they implement, as queries and notifications already do.
- #251: `Distinct()` the scanned assemblies and register handlers, processors and behaviors with `TryAddEnumerable`, so repeated markers or repeated `AddCortexMediator` calls register each implementation once per service type.
- #252: `CacheInvalidator` keeps one `CancellationTokenSource` per query type; `CachingQueryBehavior` takes an optional `ICacheInvalidator` and, when it is the built-in `CacheInvalidator`, attaches that type's change token to each entry; `InvalidateAll` cancels and replaces the source. The unused `TrackKey` and `_keysByType` (internal) go away. Rejected: tracking keys in a `HashSet` per type, because it grows without bound unless every eviction is tracked too.
- #253: build the key from `queryType.ToString()` (full name with generic arguments, without assembly versions) instead of `Name`.
- #254: route all five reflection dispatches through one helper that rethrows the inner exception of a `TargetInvocationException` with `ExceptionDispatchInfo`, which works on every target. Rejected: `BindingFlags.DoNotWrapExceptions`, missing on netstandard2.0; cached typed delegates, which belong to #264.
- #255: wrap each handler delegate so a synchronous throw becomes `Task.FromException`, in `Mediator.cs` and the generated mediator, and make `ParallelNotificationStrategy` start every delegate the same way for custom callers. The first exception is still the one thrown (today's contract).
- #256: await each validator in turn with the request's token, and skip all work when no validators are registered.
- #257: add `ExceptionHandlingOptions { SuppressNotificationExceptions }`, read through an optional trailing `IOptions<ExceptionHandlingOptions>` constructor parameter (the `bool` parameter stays, either one turns suppression on); `options.AddExceptionHandlingBehaviors(o => ...)` stores the action and `RegisterPipelineBehaviors` applies it with `services.Configure`, so `services.Configure<ExceptionHandlingOptions>` works too. Rejected: a second constructor, because two 3-parameter constructors are ambiguous to the container.

## Out of scope
- New features from the issues' notes: a closed `AddStreamQueryPipelineBehavior<T>()` (#250 notes), aggregate exceptions for parallel publishing (#255), cached typed dispatchers (#264).
- `TryAddEnumerable` in the generated DI registrations (`GeneratedDIEmitter.cs:74`, `:86`); #251 is about marker scanning.
- The `GetHashCode` fallback and the result type in cache keys (#253 "related weaknesses").
- Any change to `Cortex.Streams.Mediator`.

## Verification
- `dotnet build Cortex.sln -c Release` → exit 0, "0 Error(s)".
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~Mediator"` and the same with `-f net8.0` → exit 0, "Failed: 0" (this filter covers the Mediator, FluentValidation, Transactional, Scheduling, SourceGenerator and StreamsMediator tests).
- `dotnet run --project tools/Cortex.Mediator.SourceGenerator.Smoke` → prints "SMOKE OK".
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj` once, unfiltered, for the record; Streams windowing failures that also occur on 1bd8088 are reported as pre-existing.
- End to end: the eight new regression tests (one per issue, named in each task) pass on both runtimes, and each failed before its fix.

## Orchestration
- Consent: Review panel (2026-11-22) · Estimate: 9 agents, 4 waves, about 63 cards (6 extra), ≈2.8× Solo tokens
- Companions: researcher=researcher-2 · software-architect=software-architect-2 · developer=developer-2 · code-reviewer=code-reviewer · security-reviewer=— · qa-engineer=— · debugger=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Review | code-reviewer × 4 (correctness, spec, tests and silent failures, simplify) | read-only, code tools, 25 | READ_FIRST sophon-scratch-review.diff + this plan | FINDINGS at THRESHOLD 80, only the diff | pending |
| 2 | Refute | code-reviewer × 3 (reachability, impact, defenses) | read-only, code tools, 15 | each Critical and Important finding | ≥2 of 3 CONFIRMED | pending |
| 3 | Re-review | code-reviewer × 1 | read-only, code tools, 15 | diff of the fix commits | ALL_ADDRESSED | pending |
| 4 | Critic | code-reviewer × 1, MODE completeness | read-only, code tools, 15 | request, Goal, Verification, commits | COMPLETE | pending |
## Tasks
### Task 1: Register closed command behaviors against their closed interfaces (#250)
- [x] Done
Files: `src/Cortex.Mediator/DependencyInjection/ServiceCollectionExtensions.cs` (modify), `src/Cortex.Tests/Mediator/Tests/MediatorRegistrationTests.cs` (create)
Depends on: none
- [x] Add `ClosedCommandBehavior_IsRegisteredAndRunsOnlyForItsCommand` (returning command, behavior adds 100; another command is unaffected) and `ClosedVoidCommandBehavior_IsRegisteredAndRuns`; run them red (`BuildServiceProvider` throws `ArgumentException`).
- [x] In `RegisterPipelineBehaviors`, register open command behavior types against the open interfaces and closed ones against each implemented closed `ICommandPipelineBehavior<,>` / `ICommandPipelineBehavior<>`, mirroring the query branch.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~MediatorRegistrationTests|FullyQualifiedName~PipelineBehaviorOrderingTests|FullyQualifiedName~MediatorPipelineTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): register closed command pipeline behaviors (#250)

### Task 2: Register each handler, processor and behavior once (#251)
- [x] Done
Files: `src/Cortex.Mediator/DependencyInjection/ServiceCollectionExtensions.cs` (modify), `src/Cortex.Tests/Mediator/Tests/MediatorRegistrationTests.cs` (modify)
Depends on: Task 1
- [x] Add `TwoMarkersInSameAssembly_NotificationHandlerRunsOnce` and `AddCortexMediatorCalledTwice_HandlersAndBehaviorsRunOnce`; run them red (handler runs twice).
- [x] `Distinct()` the assemblies in `RegisterHandlers` and `RegisterProcessors`; use `services.TryAddEnumerable(new ServiceDescriptor(...))` in `ScanAndRegister` and for every behavior registration in `RegisterPipelineBehaviors`.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~MediatorRegistrationTests|FullyQualifiedName~HandlerLifetimeTests|FullyQualifiedName~PipelineBehaviorOrderingTests|FullyQualifiedName~ProcessorIntegrationTests|FullyQualifiedName~NotificationBehaviorIntegrationTests|FullyQualifiedName~QueryBehaviorIntegrationTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): register handlers and behaviors once per assembly (#251)

### Task 3: Make InvalidateAll remove cached results (#252)
- [x] Done
Files: `src/Cortex.Mediator/Caching/CacheInvalidator.cs` (modify), `src/Cortex.Mediator/Behaviors/CachingQueryBehavior.cs` (modify), `src/Cortex.Tests/Mediator/Tests/CachingIntegrationTests.cs` (modify)
Depends on: Task 2
- [x] Add `InvalidateAll_RemovesEveryCachedResultOfThatQueryType` (two cached keys of one type, one of another type; after `InvalidateAll` the first two miss, the other still hits); run it red.
- [x] `CacheInvalidator`: replace `_keysByType`/`TrackKey` with a `ConcurrentDictionary<Type, CancellationTokenSource>` and an internal `GetExpirationToken(Type)`; `InvalidateAll` removes and cancels the type's source.
- [x] `CachingQueryBehavior`: optional trailing `ICacheInvalidator cacheInvalidator = null` parameter; when it is a `CacheInvalidator`, add its token to `MemoryCacheEntryOptions.ExpirationTokens`.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~Caching|FullyQualifiedName~CacheInvalidatorTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): make InvalidateAll remove cached query results (#252)

### Task 4: Use the full query type name in cache keys (#253)
- [x] Done
Files: `src/Cortex.Mediator/Caching/DefaultCacheKeyGenerator.cs` (modify), `src/Cortex.Tests/Mediator/Tests/DefaultCacheKeyGeneratorTests.cs` (modify)
Depends on: Task 3
- [x] Add `SameNameQueriesInDifferentNamespaces_GetDifferentKeys` (all three key shapes: custom key, property hash, no properties) and `GenericQueries_WithDifferentArguments_GetDifferentKeys`; run them red.
- [x] Replace `queryType.Name` with `queryType.ToString()` in the three key formats.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~Caching|FullyQualifiedName~DefaultCacheKeyGeneratorTests|FullyQualifiedName~CacheInvalidatorTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): use the full query type name in cache keys (#253)

### Task 5: Rethrow the original exception from runtime-typed dispatch (#254)
- [ ] Done
Files: `src/Cortex.Mediator/Mediator.cs` (modify), `src/Cortex.Tests/Mediator/Tests/StreamingQueryTests.cs` (modify)
Depends on: Task 4
- [ ] Add `CreateStream_RuntimeTyped_WithoutHandler_ThrowsInvalidOperationException` and `CreateStream_RuntimeTyped_BehaviorThrowsSynchronously_ThrowsOriginalException`; run them red (`TargetInvocationException`).
- [ ] Add a private static `InvokeUnwrapped(MethodInfo, object, object[])` that catches `TargetInvocationException` with a non-null inner exception and rethrows it via `ExceptionDispatchInfo.Capture(...).Throw()`; use it in all five runtime-typed overloads.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~StreamingQueryTests|FullyQualifiedName~StreamingIntegrationTests|FullyQualifiedName~MediatorTests|FullyQualifiedName~RuntimeNotificationDispatchTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): rethrow original exceptions from runtime-typed dispatch (#254)

### Task 6: Run every handler when one throws synchronously in parallel publishing (#255)
- [ ] Done
Files: `src/Cortex.Mediator/Mediator.cs` (modify), `src/Cortex.Mediator/Notifications/ParallelNotificationStrategy.cs` (modify), `src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedMediatorEmitter.cs` (modify), `src/Cortex.Tests/Mediator/Tests/NotificationPublishStrategyTests.cs` (modify), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorRuntimeTests.cs` (modify)
Depends on: Task 5
- [ ] Add `Parallel_HandlerThrowsSynchronously_OtherHandlersStillRun` (reflection mediator, default strategy, throwing handler registered first), `Parallel_Strategy_DelegateThrowsSynchronously_StartsTheRest` (strategy alone) and a generated-mediator runtime test with the same scenario; run them red.
- [ ] In `Mediator.PublishAsync` and the emitted `PublishAsync`, build each final delegate as `() => { try { return finalDelegate(); } catch (Exception ex) { return Task.FromException(ex); } }`; in `ParallelNotificationStrategy`, start each delegate inside the same try/catch before `Task.WhenAll`.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~NotificationPublishStrategyTests|FullyQualifiedName~NotificationPipelineTests|FullyQualifiedName~GeneratedMediator|FullyQualifiedName~GeneratorDriverTests|FullyQualifiedName~ExceptionHandlingNotificationBehaviorTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): run every handler when one throws synchronously (#255)

### Task 7: Await FluentValidation validators with the request's token (#256)
- [ ] Done
Files: `src/Cortex.Mediator.Behaviors.FluentValidation/ValidationCommandBehavior.cs` (modify), `src/Cortex.Mediator.Behaviors.FluentValidation/ValidationQueryBehavior.cs` (modify), `src/Cortex.Mediator.Behaviors.FluentValidation/VoidValidationCommandBehavior.cs` (modify), `src/Cortex.Tests/Mediator/FluentValidation/Tests/ValidationCommandBehaviorTests.cs` (modify), `src/Cortex.Tests/Mediator/FluentValidation/Tests/ValidationQueryBehaviorTests.cs` (modify), `src/Cortex.Tests/Mediator/FluentValidation/Tests/VoidValidationCommandBehaviorTests.cs` (modify)
Depends on: Task 6
- [ ] In each test class add `Handle_PassesCancellationTokenToValidators` (a `MustAsync((x, ct) => ...)` rule captures the token) and `Handle_DoesNotBlockWhileAsyncValidatorIsPending` (calling `Handle` on a worker thread returns within 5 s while the validator awaits a `TaskCompletionSource`; the source is completed afterwards either way); run them red.
- [ ] Replace the `Select(...).Select(r => r.Result)` chain with a loop that awaits `validator.ValidateAsync(context, cancellationToken)` per validator, collecting non-null errors; return `next()` early when there are no validators. The thrown `ValidationException` and its dictionary stay the same.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~ValidationCommandBehaviorTests|FullyQualifiedName~ValidationQueryBehaviorTests|FullyQualifiedName~VoidValidationCommandBehaviorTests"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator-validation): await validators with the cancellation token (#256)

### Task 8: Let notification exception suppression be configured (#257)
- [ ] Done
Files: `src/Cortex.Mediator/Behaviors/ExceptionHandlingOptions.cs` (create), `src/Cortex.Mediator/Behaviors/ExceptionHandlingNotificationBehavior.cs` (modify), `src/Cortex.Mediator/DependencyInjection/MediatorOptions.cs` (modify), `src/Cortex.Mediator/DependencyInjection/MediatorOptionsExtensions.cs` (modify), `src/Cortex.Mediator/DependencyInjection/ServiceCollectionExtensions.cs` (modify), `src/Cortex.Mediator/README.md` (modify), `src/Cortex.Tests/Mediator/Tests/ExceptionHandlingIntegrationTests.cs` (modify)
Depends on: Task 7
- [ ] Add `SuppressNotificationExceptions_ConfiguredThroughOptions_LetsOtherHandlersRun` (sequential strategy, first handler throws, `AddExceptionHandlingBehaviors(o => o.SuppressNotificationExceptions = true)`; publish completes and the second handler ran), `SuppressNotificationExceptions_ConfiguredThroughServicesConfigure_LetsOtherHandlersRun`, and `SuppressNotificationExceptions_Default_Rethrows`; run them red (compile failure, the options type does not exist).
- [ ] Create `ExceptionHandlingOptions` (namespace `Cortex.Mediator.Behaviors`); add the optional trailing `IOptions<ExceptionHandlingOptions> options = null` parameter to the behavior; add the `AddExceptionHandlingBehaviors(this MediatorOptions, Action<ExceptionHandlingOptions>)` overload storing an internal action on `MediatorOptions`; apply it with `services.Configure` in `RegisterPipelineBehaviors`.
- [ ] Replace the README placeholder at "Notification Exception Suppression" with both configuration forms and a note that suppression only helps other handlers run with a sequential strategy or parallel publishing.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~ExceptionHandling"`
Expected: exit 0, "Failed: 0"
Commit: fix(mediator): allow notification exception suppression to be configured (#257)

## Risks
- `TryAddEnumerable` changes registration for anyone who registered the same behavior type twice on purpose; that now runs once. Mentioned in the PR as a behaviour change.
- Cache keys change format (#253): one round of cache misses after upgrading, and code that built keys by hand for `InvalidateByKey` must switch to the full name. Mentioned in the PR.
- The generated-mediator emitter change affects every consumer's generated code; covered by the runtime test, the driver tests and the smoke project.
- Streams windowing tests are flaky (AGENTS.md); the final full run may show failures unrelated to this branch.

## Progress
- 2026-11-22 Task 1: red — the two new tests failed on 1bd8088 ("Failed: 2, Passed: 0").
- 2026-11-22 Ruling: one private `RegisterBehavior` helper now registers all five behavior kinds (query and notification logic moved into it unchanged) — Task 2 needs one place to switch to `TryAddEnumerable` — cost if wrong: none, same registrations.
- 2026-11-22 Task 1: complete — 5 test classes → "Failed: 0, Passed: 36" (commit 135f0b0).
- 2026-11-22 Task 2: red — with the 1bd8088 version of ServiceCollectionExtensions.cs swapped in temporarily, the two new tests failed ("Failed: 2, Passed: 2"); fixed version restored.
- 2026-11-22 Task 2: repair 1/3 — the first verify run still failed because restoring the file with `copy` kept its old timestamp and MSBuild skipped the rebuild → touched the file; tests then passed.
- 2026-11-22 Task 2: complete — `--filter "FullyQualifiedName~Mediator"` → "Failed: 0, Passed: 417" (commit f5b2c32).
- 2026-11-22 Task 3: red — `InvalidateAll_RemovesEveryCachedResultOfThatQueryType` and `InvalidateAll_ThenNewResults_AreCachedAgain` failed ("Failed: 2, Passed: 6").
- 2026-11-22 Ruling: the caching behavior takes its invalidation token before running the handler — an `InvalidateAll` during the handler then also expires that result — cost if wrong: one extra cache miss in that race.
- 2026-11-22 Task 3: complete — caching filter → "Failed: 0, Passed: 20" (commit 2a64089).
- 2026-11-22 Task 4: red — 5 new test cases failed, plus `GenerateKey_WithoutProperties_ShouldStillWork`, whose expected key was updated from `"Test:TestQuery"` to the full type name ("Failed: 6, Passed: 7").
- 2026-11-22 Task 4: complete — see the commit's run.
