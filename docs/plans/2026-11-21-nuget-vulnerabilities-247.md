# Plan: Fix the 6 known NuGet vulnerabilities and the SqlClient NU1701 warning (#247, part 1)
Status: in-progress
Date: 2026-11-21 · Agent: lead-developer · Size: medium · Base: v3.2/release@71768f40 · Branch: v3/bug/247
Approved: 2026-11-21 via info.request
Consent: solo
Execution: inline
Request: "Within the version v3.2 now we would like to implement the bug issue we have with id #247 in Github, lets analyze it and fix the issue, and create a PR to merge with v3.2/release"

## Goal
`dotnet list Cortex.sln package --vulnerable --include-transitive` reports no vulnerable packages for every project, restore logs no NU1701 for Microsoft.Data.SqlClient, and the solution builds and Cortex.Tests passes as before.
## Context
- Issue #247 (https://github.com/buildersoftio/cortex/issues/247): part 1 = fix 6 vulnerabilities (3 high, 3 moderate) in 5 packages across 7 projects, plus NU1701 on Cortex.States.MSSqlServer net7.0. Part 2 (outdated refresh) asks for "one PR per connector or family for the majors" → out of scope here.
- Audit reproduced on 71768f40 with SDK 10.0.401: exactly the issue's findings (OpenTelemetry.Api 1.11.2; SQLitePCLRaw.lib.e_sqlite3 2.1.10 in Cortex.States.SQLite and Cortex.Tests; SharpCompress 0.30.1 + Snappier 1.0.0 in both MongoDb projects; System.Security.Cryptography.Pkcs 8.0.1/9.0.0 in both MSSqlServer projects) and the two NU1701 warnings.
- Direct references: `src/Cortex.Telemetry.OpenTelemetry/Cortex.Telemetry.OpenTelemetry.csproj:54-57` (OpenTelemetry* 1.11.2, net9.0;net8.0); `src/Cortex.States.MongoDb/Cortex.States.MongoDb.csproj:34` and `src/Cortex.Streams.MongoDb/Cortex.Streams.MongoDb.csproj:49` (MongoDB.Driver 3.3.0); `src/Cortex.States.MSSqlServer/Cortex.States.MSSqlServer.csproj:49` (net9.0;net8.0;net7.0) and `src/Cortex.Streams.MSSqlServer/Cortex.Streams.MSSqlServer.csproj:48` (Microsoft.Data.SqlClient 6.0.1); `src/Cortex.States.SQLite/Cortex.States.SQLite.csproj:48` and `src/Cortex.Tests/Cortex.Tests.csproj:18` (Microsoft.Data.Sqlite 9.0.3).
- nuget.org nuspecs checked: OpenTelemetry 1.15.3 keeps net8.0/net9.0 dependency groups (Microsoft.Extensions.* 8.0.0/9.0.0); 1.21.0 has no net8.0/net9.0 group and pulls Microsoft.Extensions.* 11.0.0 on netstandard2.0.
- MongoDB.Driver 3.14.1 depends on SharpCompress 0.48.1 and Snappier 1.3.1 (both patched).
- Microsoft.Data.SqlClient 6.1.8 depends on System.Security.Cryptography.Pkcs 8.0.2 (net8.0, netstandard2.0) and 9.0.21 (net9.0), both patched, and has a netstandard2.0 group, which net7.0 can use instead of the .NET Framework fallback that causes NU1701.
- GHSA-2m69-gcr7-jv3q: affected SQLitePCLRaw.lib.e_sqlite3 <= 2.1.11. SQLitePCLRaw.bundle_e_sqlite3 2.1.14 exists and depends on lib.e_sqlite3 2.1.14.
- No Directory.Build.props or central package management; versions live in each csproj. Only test project: src/Cortex.Tests (`src/Cortex.Tests/Cortex.Tests.csproj:30-42` references none of the 6 affected shipped projects; it uses Microsoft.Data.Sqlite directly for the scheduling tests).
- Known pre-existing test failures in Cortex.Streams (SourceOperatorErrorHandlingTests, window operators), recorded in docs/plans/2026-11-20-mediator-scheduling.md.
- Assumption: branch name `v3/bug/247` follows the repo's `v3/bug/<id>` / `v3/feature/<id>` convention.
## Approach
Smallest version moves that clear each advisory without a major bump: OpenTelemetry family 1.11.2 → 1.15.3 (aligned), MongoDB.Driver 3.3.0 → 3.14.1, Microsoft.Data.SqlClient 6.0.1 → 6.1.8 (fixes Pkcs; NU1701 goes away by dropping net7.0 from Cortex.States.MSSqlServer), and a direct SQLitePCLRaw.bundle_e_sqlite3 2.1.14 reference next to Microsoft.Data.Sqlite 9.0.3. One task per package family so each can be reverted alone.
- Rejected: OpenTelemetry 1.21.0 — drops net8.0/net9.0 targets and drags Microsoft.Extensions.* 11.0.0 into net8/net9 consumers.
- Rejected: Microsoft.Data.SqlClient 7.1.2 — major bump; issue part 2 wants majors in separate PRs.
- Dropping net7.0 from Cortex.States.MSSqlServer: first rejected, then chosen during Task 3 (see Ruling in Progress) — 6.1.8's netstandard2.0 build used by net7.0 is a stub.
- Rejected: direct System.Security.Cryptography.Pkcs / SharpCompress / Snappier pins — the parent upgrades already pull patched versions.
- Rejected: Microsoft.Data.Sqlite 11.0.0 — major bump.
## Out of scope
- Issue part 2 (outdated dependency refresh, majors).
- Version/release-note bumps of the affected packages (release workflow sets versions).
## Verification
- `dotnet list Cortex.sln package --vulnerable --include-transitive` → every project "has no vulnerable packages".
- `dotnet restore Cortex.sln` → no NU1701 line.
- `dotnet build Cortex.sln -c Release -nologo` → exit 0, warning count not above the baseline taken before Task 1.
- `dotnet test src/Cortex.Tests -nologo` → only the known pre-existing Cortex.Streams failures.
- End to end: the first command above, run on the last commit.
## Tasks
### Task 1: Bump OpenTelemetry packages to 1.15.3
- [x] Done
Files: `src/Cortex.Telemetry.OpenTelemetry/Cortex.Telemetry.OpenTelemetry.csproj` (modify)
Depends on: none
- [x] Baseline: `dotnet build Cortex.sln -c Release -nologo` warning count recorded in Progress before any edit
- [x] OpenTelemetry, OpenTelemetry.Api, OpenTelemetry.Exporter.Console, OpenTelemetry.Extensions.Hosting all at 1.15.3
Run: `dotnet build src/Cortex.Telemetry.OpenTelemetry -c Release -nologo && dotnet list src/Cortex.Telemetry.OpenTelemetry package --vulnerable --include-transitive`
Expected: exit 0, "0 Error(s)", "has no vulnerable packages"
Commit: fix(telemetry): bump OpenTelemetry to 1.15.3 for GHSA-g94r-2vxg-569j
### Task 2: Bump MongoDB.Driver to 3.14.1
- [x] Done
Files: `src/Cortex.States.MongoDb/Cortex.States.MongoDb.csproj` (modify), `src/Cortex.Streams.MongoDb/Cortex.Streams.MongoDb.csproj` (modify)
Depends on: none
- [x] MongoDB.Driver 3.14.1 in both projects; SharpCompress resolves 0.48.1, Snappier 1.3.1
Run: `dotnet build src/Cortex.States.MongoDb -c Release -nologo && dotnet build src/Cortex.Streams.MongoDb -c Release -nologo && dotnet list src/Cortex.States.MongoDb package --vulnerable --include-transitive && dotnet list src/Cortex.Streams.MongoDb package --vulnerable --include-transitive`
Expected: exit 0, "0 Error(s)" twice, "has no vulnerable packages" twice
Commit: fix(mongodb): bump MongoDB.Driver to 3.14.1 for Snappier and SharpCompress advisories
### Task 3: Bump Microsoft.Data.SqlClient to 6.1.8
- [x] Done
Files: `src/Cortex.States.MSSqlServer/Cortex.States.MSSqlServer.csproj` (modify), `src/Cortex.Streams.MSSqlServer/Cortex.Streams.MSSqlServer.csproj` (modify)
Depends on: none
- [x] Microsoft.Data.SqlClient 6.1.8 in both projects; Pkcs resolves 8.0.2 / 9.0.21
- [x] Cortex.Streams.MSSqlServer: Microsoft.Extensions.Logging.Abstractions 9.0.3 → 9.0.21 (needed by SqlClient 6.1.8 on net9.0)
- [x] Cortex.States.MSSqlServer targets net9.0;net8.0 (net7.0 dropped), so no NU1701
Run: `dotnet build src/Cortex.States.MSSqlServer -c Release -nologo && dotnet build src/Cortex.Streams.MSSqlServer -c Release -nologo && dotnet list src/Cortex.States.MSSqlServer package --vulnerable --include-transitive && dotnet list src/Cortex.Streams.MSSqlServer package --vulnerable --include-transitive`
Expected: exit 0, "0 Error(s)", no "NU1701" in the output, "has no vulnerable packages" twice
Commit: fix(mssql): bump Microsoft.Data.SqlClient to 6.1.8 and drop net7.0
### Task 4: Pin a patched SQLitePCLRaw bundle
- [x] Done
Files: `src/Cortex.States.SQLite/Cortex.States.SQLite.csproj` (modify), `src/Cortex.Tests/Cortex.Tests.csproj` (modify)
Depends on: none
- [x] Direct `SQLitePCLRaw.bundle_e_sqlite3` 2.1.14 reference next to Microsoft.Data.Sqlite 9.0.3 in both projects; lib.e_sqlite3 resolves 2.1.14
Run: `dotnet build src/Cortex.States.SQLite -c Release -nologo && dotnet list src/Cortex.States.SQLite package --vulnerable --include-transitive && dotnet list src/Cortex.Tests package --vulnerable --include-transitive && dotnet test src/Cortex.Tests -nologo --filter FullyQualifiedName~Scheduling`
Expected: exit 0, "has no vulnerable packages" twice, scheduling tests "Failed: 0"
Commit: fix(sqlite): pin SQLitePCLRaw.bundle_e_sqlite3 2.1.14 for CVE-2025-6965
### Task 5: Enable NuGet audit for transitive packages (approved: warnings only)
- [ ] Done
Files: `Directory.Build.props` (create)
Depends on: Task 1, Task 2, Task 3, Task 4
- [ ] `NuGetAudit` true and `NuGetAuditMode` all for every project
Run: `dotnet restore Cortex.sln`
Expected: exit 0, no NU1901-NU1904 lines
Commit: build: audit transitive NuGet packages on restore
## Risks
- MongoDB.Driver 3.3 → 3.14 is eleven minor versions; only compile-checked, no Mongo integration tests exist.
- Cortex.States.MSSqlServer no longer ships a net7.0 build; net7.0 consumers keep the previous package version.
- Neither SQL Server, MongoDB nor OpenTelemetry projects are covered by Cortex.Tests.
## Progress
- 2026-11-21 Ruling: Task 5 warnings only, no WarningsAsErrors — user's Audit answer — cost if wrong: one line to add later
- 2026-11-21 Baseline: `dotnet build Cortex.sln -c Release -nologo` on 71768f40 → "615 Warning(s)", "0 Error(s)"
- 2026-11-21 Task 1: complete — `dotnet build src/Cortex.Telemetry.OpenTelemetry -c Release` + `dotnet list … --vulnerable` → 0 Error(s), no vulnerable packages
- 2026-11-21 Task 2: complete — both MongoDb builds + `dotnet list … --vulnerable` → 0 Error(s), no vulnerable packages
- 2026-11-21 Ruling: drop net7.0 from Cortex.States.MSSqlServer (user chose it via info.request) — SqlClient 6.1.8 resolves lib/netstandard2.0/Microsoft.Data.SqlClient.dll for net7.0 (79,768 bytes, no runtimes/ builds; the net8.0 win build is 2,176,960 bytes), a stub — cost if wrong: net7.0 users of the next release lose this package
- 2026-11-21 Task 3: repair 1/3 — Cortex.Streams.MSSqlServer build: "error NU1605: Detected package downgrade: Microsoft.Extensions.Logging.Abstractions from 9.0.21 to 9.0.3" (SqlClient 6.1.8 → Caching.Memory 9.0.21 on net9.0) → direct reference bumped to 9.0.21
- 2026-11-21 Ruling: Logging.Abstractions 9.0.3 → 9.0.21 in Cortex.Streams.MSSqlServer — patch bump on the same 9.0 line, forced by SqlClient 6.1.8 — cost if wrong: none expected
- 2026-11-21 Task 3: complete — both MSSqlServer builds, forced restore without NU1701, `dotnet list … --vulnerable` → no vulnerable packages
- 2026-11-21 Task 4: complete — Cortex.States.SQLite build, `dotnet list … --vulnerable` for it and Cortex.Tests, scheduling tests → no vulnerable packages, Failed: 0
