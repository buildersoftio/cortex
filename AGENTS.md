# AGENTS.md

Cortex Data Framework is a set of .NET libraries published to NuGet by Buildersoft. It covers stream processing (`Cortex.Streams` plus connectors for Kafka, Pulsar, RabbitMQ, SQS, Azure, S3, HTTP, files and CDC), state stores (`Cortex.States` plus RocksDB, SQL, Mongo, Cassandra, ClickHouse, DuckDB and SQLite), and a mediator/CQRS stack (`Cortex.Mediator` with behaviors, a Roslyn source generator and scheduling). It also includes `Types`, `Vectors`, `Serialization.Yaml` and `Telemetry`. The repo contains libraries only, so there is no application to run.

## Toolchain
- The .NET 11 SDK is required, because projects target `net11.0` (CI uses `11.0.x`). There is no `global.json`.
- Running the whole test suite needs the .NET 8 and .NET 11 runtimes, because `Cortex.Tests` targets `net11.0;net8.0`.

## Commands (run from the repo root)
```
dotnet build Cortex.sln -c Release                      # what CI runs; also packs every library (see Pitfalls)
dotnet test src/Cortex.Tests/Cortex.Tests.csproj        # all tests, net11.0 and net8.0 (~35 s each)
dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~OneOfTests"
dotnet test src/Cortex.Tests/Cortex.Tests.csproj -f net11.0 --filter "FullyQualifiedName~MediatorTests.SendAsync_WithVoidCommand_ShouldExecuteHandler"
dotnet run --project tools/Cortex.Mediator.SourceGenerator.Smoke   # source-generator consumer smoke test, prints "SMOKE OK"
```
- Filter tests by class or method name, not by namespace. Test namespaces are inconsistent: some are `Cortex.Tests.Mediator.Tests` and others are `Cortex.Streams.Tests`.
- No lint or format tooling is configured: there is no `.editorconfig`, no analyzer package and no `TreatWarningsAsErrors`. Match the style of the surrounding code.
- `Directory.Build.props` turns on `NuGetAudit` with `NuGetAuditMode=all`, so restore warns about vulnerable transitive packages too.

## Layout
- `Cortex.sln` contains 35 projects: one per package under `src/Cortex.*`, plus `src/Cortex.Tests`.
- Files to open first:
  - `src/Cortex.Streams/StreamBuilder.cs`, `Stream.cs` and `Operators/`: the pipeline API and operators.
  - `src/Cortex.States/Abstractions/` and `InMemoryStateStore.cs`
  - `src/Cortex.Mediator/Mediator.cs`, `DependencyInjection/` and `Behaviors/`
  - `src/Cortex.Mediator.SourceGenerator/CortexMediatorGenerator.cs`
  - `src/Cortex.Mediator.Scheduling/ScheduledCommandProcessor.cs` and `Stores/`
- `src/Cortex.Tests/<Area>/Tests/*Tests.cs` holds the tests for one area: Mediator (including FluentValidation, Transactional, Scheduling and SourceGenerator), Streams, StreamsMediator, States, Types, Serialization and Streams.Kafka. The tests use xUnit 2.9 and Moq, with a global `using Xunit`. Only the projects referenced in `Cortex.Tests.csproj` have tests. Most connector and state-store provider packages have none.
- `tools/` is not part of the solution. `KafkaSinkDiagnostic.cs` is a standalone file and does not belong to any project.
- `docs/wiki/` holds feature docs. `docs/plans/YYYY-MM-DD-<topic>.md` holds work plans with a Status/Date/Branch header.
- `.github/workflows/sdk-release.yml` is the only workflow. It runs when a PR to `v*/main` is merged and does build, pack, NuGet push, tag and GitHub release. **There is no PR or CI test job, so run the tests yourself.**

## Conventions
- Multi-targeting: each `.csproj` declares its own `<TargetFrameworks>` (`net11.0` down to `netstandard2.0`, depending on the package). Code has to compile for every TFM in the list. Use `#if` or the existing `Polyfills/` instead of dropping a target, because dropping a TFM is a breaking change.
- C# version varies per project:
  - `Cortex.Streams` is pinned to `LangVersion 8.0`.
  - `Cortex.Types` and `Cortex.Serialization.Yaml` use 9.
  - The `Cortex.Mediator*` projects use 12.
  - Projects without `LangVersion` get the TFM default. This means C# 7.3 for the `netstandard2.0` builds of `Cortex.States` and `Cortex.Telemetry`.
- `<Nullable>enable` is set only in some projects (the Streams cloud and file connectors, Kafka, Pulsar, RabbitMQ, S3 and Telemetry.OpenTelemetry). Don't add nullable annotations to projects that don't have it.
- A new package needs:
  - the same csproj metadata block as its siblings,
  - an `Assets/license.md` and an icon packed at the root,
  - a README, packed either from the root `README.md` or from its own `README.md` (the Mediator family, Pulsar, Vectors and Yaml use their own),
  - an entry in `Cortex.sln`,
  - an entry with a NuGet badge under "Project Structure" in the root `README.md`.
- Commits use Conventional Commits with a scope, for example `fix(streams): …`, `feat(states): …`, `test: …`, `ci: …` or `docs(plans): …`. Branches are named `v3/feature/<issue>` or `v3/bug/<issue>`, and PRs go into the current `v3.x/release` branch.

## Pitfalls
- `GeneratePackageOnBuild=True` is set in every library, so each build also writes `.nupkg` files. If pack metadata or assets are broken, the build fails.
- Don't hand-edit `<Version>` to release. CI overwrites `<Version>` in every csproj with the next tag for the major version (a PR with the `feature` label bumps minor, anything else bumps patch).
- The root `README.md` is the NuGet readme for most packages, so editing it changes those package pages.
- The windowing and timing tests in `Streams/Tests` (`AdvancedWindowingTests`, `TumblingWindowOperatorTests`) are flaky under load and fail in a different place on each full run. Rerun them alone before treating a failure as real.
- The 6 tests in `Streams.Kafka` are always skipped because they need a live Kafka broker. Scheduling store tests run live only against SQLite.
- Some package IDs differ from their folder names: `src/Cortex.Streams.MSSqlServer`, `src/Cortex.Streams.PostgreSQL` and `src/Cortex.Streams.MongoDb` ship as `Cortex.Streams.CDC.*`.
- To reference the source generator from a project, use `OutputItemType="Analyzer"`, as `Cortex.Tests.csproj` and the smoke project do. It targets `netstandard2.0` with Roslyn 4.3.1, so keep it compatible with both.
