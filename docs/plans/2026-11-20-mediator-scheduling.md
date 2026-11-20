# Plan: Cortex.Mediator.Scheduling — deferred and scheduled commands (#228)
Status: in-progress
Date: 2026-11-20 · Agent: lead-developer · Size: large · Base: v3.2/release@47f87dae · Branch: v3/feature/228
Approved: 2026-11-20 via info.request
Consent: Review panel (2026-11-20)
Execution: inline
Request: "In the journey to release v3.2, we need to implement a new feature that is described in the following issue in github #228. After the planning and implementation lets create a PR towards v3.2/release."

## Goal
A new `Cortex.Mediator.Scheduling` package lets callers schedule an `ICommand` for a delay or an absolute time, cancel it, and have it dispatched through `IMediator` by a hosted processor, backed by an in-memory store or a database table, with documented hooks for Hangfire / Quartz.NET.
## Context
- Issue #228 (https://github.com/buildersoftio/cortex/issues/228) asks for: `ICommandScheduler` with `ScheduleAsync<TCommand>(command, TimeSpan delay, ct)`, `ScheduleAsync<TCommand>(command, DateTimeOffset executeAt, ct)` (both `where TCommand : ICommand`) and `CancelAsync(string scheduleId, ct)`; an in-memory implementation; a persistent implementation using a database table; `ScheduledCommandProcessor` as an `IHostedService` that polls and dispatches due commands; optional Hangfire / Quartz.NET integration hooks.
- Void command marker: `src/Cortex.Mediator/Commands/ICommand.cs:18`. Runtime-typed dispatch exists: `IMediator.SendCommandAsync(ICommand, ct)` at `src/Cortex.Mediator/IMediator.cs:56-58`, implemented by reflection at `src/Cortex.Mediator/Mediator.cs:84-105`.
- `IMediator` is registered scoped: `src/Cortex.Mediator/DependencyInjection/ServiceCollectionExtensions.cs:25`, so a background processor must create a DI scope per dispatch.
- Add-on package pattern to copy: `src/Cortex.Mediator.Behaviors.Transactional/Cortex.Mediator.Behaviors.Transactional.csproj:1-69` (net10.0;net9.0;net8.0, LangVersion 12, version 3.0.0, Assets/cortex.png + Assets/license.md, README packed, ProjectReference to Cortex.Mediator); DI extension style at `.../DependencyInjection/ServiceCollectionExtensions.cs:17-38`.
- Repo dependency versions: Microsoft.Extensions.* 9.0.3 (`src/Cortex.Mediator/Cortex.Mediator.csproj:64-70`); Microsoft.Data.Sqlite 9.0.3 (`src/Cortex.States.SQLite/Cortex.States.SQLite.csproj:48`).
- Tests: single xUnit project `src/Cortex.Tests/Cortex.Tests.csproj` (net8.0, Moq, xunit 2.9.3); mediator add-on tests live under `src/Cortex.Tests/Mediator/<Area>/Tests`. CONTRIBUTING.md:78 "Include or update tests for your changes."; CONTRIBUTING.md:97 "Add or modify tests in the `Cortex.Tests` project."
- Branch naming in this repo: `v3/feature/<issue>` (e.g. v3/feature/214, v3/feature/225), merged into `v3.2/release`.
- SDK 10.0.401 installed.
- User choices (2026-11-20): persistence = one ADO.NET store with SQL Server / PostgreSQL / SQLite dialects; Hangfire / Quartz = hooks plus README samples, no adapter packages.
- Assumption: only void `ICommand` is schedulable, as the issue's signatures state; `ICommand<TResult>` has no caller to receive a result.
- Assumption: targeting net8.0+ (like Transactional) lets the package use the built-in `TimeProvider` for testable time without extra dependencies.
## Approach
New project `src/Cortex.Mediator.Scheduling` (net10.0;net9.0;net8.0). `CommandScheduler` implements `ICommandScheduler`: it validates input, serializes the command with `IScheduledCommandSerializer` (System.Text.Json, type stored as "FullName, AssemblyName" and accepted on read only if it implements `ICommand`), and writes a `ScheduledCommand` record (Id, CommandType, Payload, ExecuteAt, CreatedAt, Status, Attempts, LastError, LockedUntil) to `IScheduledCommandStore`. Stores: `InMemoryScheduledCommandStore` (lock-guarded dictionary) and `DbScheduledCommandStore` (ADO.NET `DbConnection` factory, parameterised SQL, times stored as UTC ticks, table name validated as an identifier, optional auto-create) with `SqlServerDialect`, `PostgreSqlDialect`, `SqliteDialect`. Claiming is atomic per row (`UPDATE … WHERE Id=@id AND claimable`), so several processor instances never dispatch the same row twice; a row locked longer than `LockDuration` becomes claimable again after a crash. `ScheduledCommandProcessor` (a `BackgroundService`) polls every `PollingInterval`, claims up to `BatchSize` due rows, dispatches each in a new DI scope via `IMediator.SendCommandAsync(ICommand)`, marks it Completed, or on error retries after `RetryDelay` until `MaxAttempts`, then Failed. Its public `ProcessDueCommandsAsync` is the Hangfire / Quartz hook: disable the hosted processor and call it from a recurring job, or replace `ICommandScheduler`; the README shows both. DI: `services.AddCortexMediatorScheduling(o => …)` and `services.AddDbScheduledCommandStore(sp => new SqlConnection(cs), new SqlServerDialect(), o => …)`.
Rejected: EF Core store — a heavy dependency and model-coupling for one table.
Rejected: one NuGet package per database — triples packaging for three small SQL dialects.
Rejected: Hangfire / Quartz adapter packages now — new third-party dependencies; the hook covers the integration.
## Out of scope
- Scheduling `ICommand<TResult>`, recurring/cron schedules, Hangfire/Quartz adapter packages.
- Live tests against SQL Server or PostgreSQL (no servers here): those dialects are compiled and reviewed only.
- CI workflow changes, version bumps of other packages, publishing to NuGet.
## Verification
- `dotnet build src/Cortex.Mediator.Scheduling/Cortex.Mediator.Scheduling.csproj -c Release` → exit 0 for net8.0, net9.0, net10.0, package produced.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.Scheduling` → 0 failed, exit 0.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj` → 0 failed, exit 0 (pre-existing failures named separately).
- End to end: a test builds a ServiceCollection with `AddCortexMediator` + `AddCortexMediatorScheduling` + the SQLite store, schedules a command, advances a fake clock, runs the processor, and observes the handler ran exactly once and the row is Completed.
## Orchestration
- Consent: Review panel (2026-11-20) · Estimate: 9 agents, 4 waves, about 47 cards (6 extra), ≈3.7× Solo tokens
- Companions: researcher=researcher-2 · software-architect=software-architect-2 · developer=developer-2 · code-reviewer=code-reviewer · security-reviewer=security-reviewer · qa-engineer=qa-engineer-2 · debugger=debugger · ui-ux-designer=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Review | 3 code-reviewer (correctness, spec, tests and silent failures) + 1 security-reviewer | read-only | sophon-scratch-review.diff | FINDINGS at threshold 80 | pending |
| 2 | Refute | 3 code-reviewer (reachability, impact, defenses) or 3 security-reviewer verify | read-only | Critical/Important findings | ≥2 of 3 CONFIRMED | pending |
| 3 | Re-review | 1 code-reviewer | read-only | fix-commit diff | ALL_ADDRESSED | pending |
| 4 | Critic | 1 code-reviewer completeness | read-only | request, Goal, Verification, commits | COMPLETE | pending |
## Tasks
### Task 1: Scaffold the package with contracts and the JSON serializer
- [x] Done
Files: `src/Cortex.Mediator.Scheduling/Cortex.Mediator.Scheduling.csproj` (create), `src/Cortex.Mediator.Scheduling/Assets/cortex.png` (create, copy), `src/Cortex.Mediator.Scheduling/Assets/license.md` (create, copy), `src/Cortex.Mediator.Scheduling/README.md` (create, stub), `src/Cortex.Mediator.Scheduling/ICommandScheduler.cs` (create), `src/Cortex.Mediator.Scheduling/ScheduledCommand.cs` (create), `src/Cortex.Mediator.Scheduling/ScheduledCommandStatus.cs` (create), `src/Cortex.Mediator.Scheduling/Stores/IScheduledCommandStore.cs` (create), `src/Cortex.Mediator.Scheduling/Serialization/IScheduledCommandSerializer.cs` (create), `src/Cortex.Mediator.Scheduling/Serialization/JsonScheduledCommandSerializer.cs` (create), `Cortex.sln` (modify), `src/Cortex.Tests/Cortex.Tests.csproj` (modify), `src/Cortex.Tests/Mediator/Scheduling/Tests/JsonScheduledCommandSerializerTests.cs` (create), `docs/plans/2026-11-20-mediator-scheduling.md` (modify)
Depends on: none
- [x] csproj mirrors Transactional (TFMs, metadata, Assets, README pack), references Cortex.Mediator and Microsoft.Extensions.Hosting.Abstractions 9.0.3; added to Cortex.sln under the Mediator folder and to Cortex.Tests.
- [x] Serializer round-trips a command; rejects a stored type that does not implement `ICommand`, and an unknown type, with a clear exception.
Run: `dotnet build src/Cortex.Mediator.Scheduling/Cortex.Mediator.Scheduling.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.Scheduling`
Expected: build exit 0; tests 0 failed, exit 0.
Commit: feat(scheduling): add Cortex.Mediator.Scheduling contracts and serializer
### Task 2: Add the in-memory store and CommandScheduler
- [ ] Done
Files: `src/Cortex.Mediator.Scheduling/Stores/InMemoryScheduledCommandStore.cs` (create), `src/Cortex.Mediator.Scheduling/CommandScheduler.cs` (create), `src/Cortex.Tests/Mediator/Scheduling/Tests/InMemoryScheduledCommandStoreTests.cs` (create), `src/Cortex.Tests/Mediator/Scheduling/Tests/CommandSchedulerTests.cs` (create), `src/Cortex.Tests/Mediator/Scheduling/Tests/FakeTimeProvider.cs` (create), `docs/plans/2026-11-20-mediator-scheduling.md` (modify)
Depends on: Task 1
- [ ] Tests first: delay and absolute schedules store the right ExecuteAt; null command → ArgumentNullException; negative delay → ArgumentOutOfRangeException; cancel of a pending row → Cancelled and never claimable; cancel of unknown/completed id is a no-op; claim returns only due rows, each once, and re-offers rows whose lock expired.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.Scheduling`
Expected: 0 failed, exit 0.
Commit: feat(scheduling): add in-memory store and command scheduler
### Task 3: Add the processor, options and DI registration
- [ ] Done
Files: `src/Cortex.Mediator.Scheduling/SchedulingOptions.cs` (create), `src/Cortex.Mediator.Scheduling/ScheduledCommandProcessor.cs` (create), `src/Cortex.Mediator.Scheduling/DependencyInjection/ServiceCollectionExtensions.cs` (create), `src/Cortex.Tests/Mediator/Scheduling/Tests/ScheduledCommandProcessorTests.cs` (create), `src/Cortex.Tests/Mediator/Scheduling/Tests/SchedulingServiceCollectionTests.cs` (create), `docs/plans/2026-11-20-mediator-scheduling.md` (modify)
Depends on: Task 2
- [ ] Tests first, through a real `AddCortexMediator` container: a due command reaches its handler once and is Completed; a not-yet-due one is not dispatched; a cancelled one is never dispatched; a throwing handler is retried after RetryDelay and becomes Failed with LastError after MaxAttempts; one failing command does not stop the rest of the batch; `EnableProcessor = false` registers no hosted service; options are validated (positive intervals, MaxAttempts ≥ 1).
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.Scheduling`
Expected: 0 failed, exit 0.
Commit: feat(scheduling): add hosted processor and DI registration
### Task 4: Add the database-table store with SQL dialects
- [ ] Done
Files: `src/Cortex.Mediator.Scheduling/Stores/DbScheduledCommandStore.cs` (create), `src/Cortex.Mediator.Scheduling/Stores/DbScheduledCommandStoreOptions.cs` (create), `src/Cortex.Mediator.Scheduling/Stores/ISqlDialect.cs` (create), `src/Cortex.Mediator.Scheduling/Stores/SqlDialects.cs` (create), `src/Cortex.Mediator.Scheduling/DependencyInjection/ServiceCollectionExtensions.cs` (modify), `src/Cortex.Tests/Cortex.Tests.csproj` (modify: Microsoft.Data.Sqlite 9.0.3), `src/Cortex.Tests/Mediator/Scheduling/Tests/DbScheduledCommandStoreTests.cs` (create), `docs/plans/2026-11-20-mediator-scheduling.md` (modify)
Depends on: Task 3
- [ ] Tests first on SQLite (temp file): table auto-created; schedule/claim/complete/fail/cancel persist across store instances; two stores claiming concurrently never return the same row; invalid table name → ArgumentException; end-to-end Goal check through the processor.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.Scheduling`
Expected: 0 failed, exit 0.
Commit: feat(scheduling): add database-table store with SQL dialects
### Task 5: Document the package and run full verification
- [ ] Done
Files: `src/Cortex.Mediator.Scheduling/README.md` (modify), `README.md` (modify: package list entry), `docs/plans/2026-11-20-mediator-scheduling.md` (modify)
Depends on: Task 4
- [ ] README: install, setup, in-memory vs database store, DDL notes per dialect, retry/lock semantics, Hangfire and Quartz.NET hook examples, limits (void commands only, JSON-serializable commands).
Run: `dotnet build src/Cortex.Mediator.Scheduling/Cortex.Mediator.Scheduling.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj`
Expected: build exit 0; full suite 0 failed, exit 0 (pre-existing failures named separately).
Commit: docs(scheduling): document Cortex.Mediator.Scheduling
## Risks
- SQL Server and PostgreSQL dialects are not run against real servers here; only SQLite is exercised.
- Commands must be JSON-serializable (public setters or constructor binding); commands with non-serializable members fail at schedule time, not at dispatch.
- New dependencies: Microsoft.Extensions.Hosting.Abstractions 9.0.3 (package); Microsoft.Data.Sqlite 9.0.3 (test project only, version already used in repo).
- At-least-once delivery: a processor crash after the handler ran but before Completed is written re-dispatches after LockDuration; handlers should be idempotent (documented).
## Progress
- 2026-11-20 Task 1: complete — `dotnet build …Scheduling.csproj -c Release && dotnet test … --filter …Mediator.Scheduling` → 0 Error(s), no warnings in the new project, Cortex.Mediator.Scheduling.3.0.0.nupkg produced; "Passed! - Failed: 0, Passed: 8".
- 2026-11-20 Ruling: branch named v3/feature/228 (repo convention) instead of sophon/<slug> — matches existing feature branches merged into v3.2/release — cost if wrong: a rename before push.
- 2026-11-20 Ruling: stored type name is "FullName, AssemblyName" (no version) and only concrete ICommand types are deserialized — survives package upgrades and refuses arbitrary types from the store — cost if wrong: commands whose assembly is renamed can no longer be read.
