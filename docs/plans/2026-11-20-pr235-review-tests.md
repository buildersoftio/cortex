# Plan: Review PR 235 and add executable generator tests
Status: in-progress
Date: 2026-11-20 · Agent: lead-developer · Size: large · Base: v3/feature/214@2a716be2 · Branch: sophon/pr235-review-tests
Approved: 2026-11-20 via info.request
Consent: Specialist team (2026-11-20)
Execution: inline
Request: "Lets analyze the open PR, and for the issue #214, and do the code review for it and add the proper testing if it is missing."

## Goal
PR #235 has executable regression coverage for issue #214, reproduced generator defects have minimal verified fixes, and native compatibility claims are backed by explicitly bounded evidence or reported as unverified.
## Context
- Issue: https://github.com/buildersoftio/cortex/issues/214; open PR: https://github.com/buildersoftio/cortex/pull/235.
- PR head: 2a716be2e65588b1ee13a7ae392183f6a8f02950; target: v3.2/release. Initial checkout is clean master@2b6ea6c. The PR object is already local; no fetch is needed.
- PR changes 24 files, with 2787 additions and 14 deletions. Existing tests primarily inspect generator diagnostics, generated text and compilation, not mediator execution: src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorIntegrationTests.cs:36-55,137-178 (PR revision).
- Class-only discovery: src/Cortex.Mediator.SourceGenerator/CortexMediatorGenerator.cs:47-65. Per-declaration collection can repeat partial symbols.
- Cases follow registration order: src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedMediatorEmitter.cs:139-151. Notification typed dispatch allows zero handlers (:270-272), while interface dispatch throws (:325-333).
- Generated handler registrations hardcode scoped lifetime: src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedDIEmitter.cs:68-75. The PR baseline exposes configurable HandlerLifetime: src/Cortex.Mediator/DependencyInjection/MediatorOptions.cs:25-27, independently checked with git show.
- Generated DI calls the runtime pipeline registration helper: src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedDIEmitter.cs:97-98. The helper reflects on configured closed behaviors; managed tests alone do not establish NativeAOT compatibility.
- Project conventions: CONTRIBUTING.md:77-78 says "Ensure your code adheres to the existing style and conventions" and "Include or update tests for your changes." CONTRIBUTING.md:94-102 specifies Cortex.Tests and dotnet test. No AGENTS.md was found.
- SDK 10.0.403 and .NET 8 runtime are installed. Native compiler prerequisites have not been checked.
- Design rubric, higher is better: meets requirements 0-3; fits patterns and reuse 0-3; contains blast radius 0-3; testability 0-3; simplicity 0-2, total 14.
- Two architect perspectives compared executable Roslyn fixtures with an isolated consumer test project. Lead aggregate scores: existing harness 25 versus isolated test project 21; choose existing harness, plus a separate console smoke for normal analyzer and native integration.
- Assumption: record and struct message implementations are in scope because the mediator interfaces do not restrict messages to classes.
- Assumption: generated dispatch is compilation-local as explicitly documented; dynamic registrations outside that contract continue to use the existing mediator fallback.
## Approach
Extend the existing Roslyn/xUnit infrastructure with uniquely named emitted assemblies and public fixture entry points that exercise generated internal DI and mediator code. Keep intentional diagnostic/compiler-negative fixtures in the generator-driver tests. Add tests before each production fix and capture red and green runs. Deduplicate semantic registrations, broaden declaration discovery where required, and provide targeted diagnostics for duplicate stream handlers and inaccessible handler types instead of incidental generated-code compiler errors. Preserve exact runtime-type dispatch, known zero-handler notification behavior, configurable handler lifetimes, pipelines, cancellation, and the legacy opt-out path. Add a separate net8 console consumer for normal analyzer integration, trimmed publishing and win-x64 native execution without introducing new dependency versions. Commit each verified task only after explicit approval of this plan, then review the branch and record final evidence.
Rejected: a new xUnit consumer project — duplicates existing Roslyn/xUnit infrastructure and increases build wiring.
Rejected: only generated-text assertions — cannot establish DI resolution, results, cancellation or pipeline behavior.
Rejected: broad runtime/AOT redesign — not justified before a concrete native failure and outside this review's bounded fixes.
## Out of scope
- Cross-assembly discovery, dynamic handler registration guarantees, public API redesign, performance benchmarks.
- New/upgraded dependencies, CI changes, native toolchain installation, deployment or publishing packages.
- Posting GitHub reviews/comments, pushing branches or opening a PR without separate approval.
- Full NuGet installation/consumer validation, all runtime identifiers, and exhaustive NativeAOT pipeline configurations.
## Verification
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator` → nonzero discovery, 0 failed, exit 0.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator` → all mediator tests pass, exit 0.
- `dotnet build src/Cortex.Tests/Cortex.Tests.csproj -c Release` → exit 0.
- `dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release` → nonzero discovery, 0 failed, exit 0; report baseline/unrelated failures separately.
- `dotnet run --project tools/Cortex.Mediator.SourceGenerator.Smoke/Cortex.Mediator.SourceGenerator.Smoke.csproj -c Release` → exit 0 and a named smoke success line.
- `dotnet publish tools/Cortex.Mediator.SourceGenerator.Smoke/Cortex.Mediator.SourceGenerator.Smoke.csproj -c Release -r win-x64 -p:PublishTrimmed=true -o tools/Cortex.Mediator.SourceGenerator.Smoke/bin/smoke-trimmed` followed by `tools\Cortex.Mediator.SourceGenerator.Smoke\bin\smoke-trimmed\Cortex.Mediator.SourceGenerator.Smoke.exe` → publish and execution exit 0; report trimming warnings.
- `dotnet publish tools/Cortex.Mediator.SourceGenerator.Smoke/Cortex.Mediator.SourceGenerator.Smoke.csproj -c Release -r win-x64 -p:PublishAot=true -o tools/Cortex.Mediator.SourceGenerator.Smoke/bin/smoke-native` followed by `tools\Cortex.Mediator.SourceGenerator.Smoke\bin\smoke-native\Cortex.Mediator.SourceGenerator.Smoke.exe` → publish and native execution exit 0; report AOT warnings and exercised scenarios only.
- End to end: real generated service registration resolves IMediator and executes all five message kinds through typed and interface entry points, with observable results, stream items, notification fan-out, cancellation and pipeline order. Native smoke separately executes a representative subset. No compatibility claim extends beyond observed checks.
## Orchestration
- Consent: Specialist team (2026-11-20) · Estimate: at most 18 agents, 9 waves, about 90 cards (10 extra), approximately 5x Solo tokens.
- Companions: researcher=researcher · software-architect=software-architect · developer=developer · code-reviewer=code-reviewer (delegation blocked) · security-reviewer=— · qa-engineer=qa-engineer · debugger=debugger · ui-ux-designer=—
| Wave | Phase | Agents | Access | Briefs | Gate | Status |
|---|---|---|---|---|---|---|
| 1 | Understand | 2 researchers | read-only | discovery/dispatch; tests/packaging | Lead rereads key PR hunks | done |
| 2 | Review | 4 code-reviewers | read-only | correctness; spec; tests; DI/parity | No children started: allow-list refusal | blocked; run inline |
| 3 | Design | 2 architects | read-only | minimal existing harness; isolated consumer | Lead checks citations and applies rubric | done |
| 4 | Implement | lead inline | write | Tasks 1-4 | Red/green and task verification | pending |
| 5 | Review | lead inline; 1 QA engineer if permitted | read-only | sequential correctness/spec/tests/simplification; QA gaps | Reproduce important findings, no unsupported claims | pending |
| 6 | Fix and re-review | lead inline | write/read | Confirmed findings only, at most 3 rounds | Recheck each fix and rerun tests | pending |
| 7 | Completeness | lead inline | read-only | Request, goal, issue requirements and evidence | Each requirement accounted for | pending |
## Tasks
### Task 1: Execute generated mediator fixtures through DI
- [x] Done
Files: `src/Cortex.Tests/Mediator/SourceGenerator/GeneratorTestHarness.cs` (create), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorRuntimeTests.cs` (create), `docs/plans/2026-11-20-pr235-review-tests.md` (modify)
Depends on: none
- [x] Create sophon/pr235-review-tests from PR head 2a716be2e65588b1ee13a7ae392183f6a8f02950 and run the focused baseline before edits.
- [x] Add a reusable existing-dependency harness that checks generator/compiler/emit diagnostics and executes uniquely named in-memory fixtures; use reflection only at the test boundary.
- [x] Verify ordinary returning/void commands, queries, stream items, notification fan-out, both overload families, cancellation propagation, pipeline order, scoped DI resolution and the opt-out legacy path.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator`
Expected: exit 0, nonzero discovered tests, 0 failed; runtime fixtures actually awaited.
Commit: test(mediator): execute generated mediator through DI
### Task 2: Cover and repair declaration and diagnostic edge cases
- [x] Done
Files: `src/Cortex.Mediator.SourceGenerator/CortexMediatorGenerator.cs` (modify), `src/Cortex.Mediator.SourceGenerator/Discovery/HandlerDiscovery.cs` (modify), `src/Cortex.Mediator.SourceGenerator/Discovery/MessageDiscovery.cs` (modify), `src/Cortex.Mediator.SourceGenerator/Models/HandlerRegistration.cs` (modify if diagnostic location metadata is needed), `src/Cortex.Mediator.SourceGenerator/Diagnostics/DiagnosticDescriptors.cs` (modify), `src/Cortex.Tests/Mediator/SourceGenerator/DiagnosticTests.cs` (modify), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratorDriverTests.cs` (modify), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorRuntimeTests.cs` (modify), `docs/plans/2026-11-20-pr235-review-tests.md` (modify)
Depends on: Task 1
- [x] Reproduce missing diagnostics for record/struct messages and missing record handler discovery, then broaden syntax selection with semantic filtering.
- [x] Reproduce repeated registrations from partial declarations; deduplicate by handler/service identity before diagnostics and emission.
- [x] Reproduce duplicate stream handlers and inaccessible nested handlers; emit targeted generator errors with source locations where available and avoid redundant generated-code errors.
- [x] Assert diagnostic IDs, severities and locations, valid output compilation, single registration/invocation, opt-in and opt-out controls.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator`
Expected: red regressions captured before fixes; final exit 0, nonzero discovery, 0 failed.
Commit: fix(mediator): handle generator declaration edge cases
### Task 3: Cover and repair dispatch and registration parity
- [ ] Done
Files: `src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedMediatorEmitter.cs` (modify), `src/Cortex.Mediator.SourceGenerator/Emitters/GeneratedDIEmitter.cs` (modify), `src/Cortex.Mediator.SourceGenerator/CortexMediatorGenerator.cs` (modify if discovered message metadata is required), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratorDriverTests.cs` (modify), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorIntegrationTests.cs` (modify only if expected source text changes), `src/Cortex.Tests/Mediator/SourceGenerator/GeneratedMediatorRuntimeTests.cs` (modify), `docs/plans/2026-11-20-pr235-review-tests.md` (modify)
Depends on: Task 2
- [ ] Reproduce base/derived dispatch compilation/order failures in both source orders, then preserve exact registered runtime-type dispatch without reflection invocation or accidental base fallback for unknown subclasses.
- [ ] Reproduce ignored HandlerLifetime for singleton/scoped/transient registrations, then honor options without changing the mediator's own scoped lifetime or transient processor lifetime.
- [ ] Reproduce interface publication failure for discovered notifications with no handlers; align it with the typed zero-handler path without hiding manually registered handlers for known message types.
- [ ] Verify null guards, unknown requests, notification fan-out/strategy, cancellation, pipeline order and legacy opt-out remain intact.
Run: `dotnet test src/Cortex.Tests/Cortex.Tests.csproj --filter FullyQualifiedName~Cortex.Tests.Mediator`
Expected: red regressions captured before fixes; final exit 0, nonzero discovery, 0 failed.
Commit: fix(mediator): preserve generated dispatch and DI parity
### Task 4: Add and verify a native consumer smoke fixture
- [ ] Done
Files: `tools/Cortex.Mediator.SourceGenerator.Smoke/Cortex.Mediator.SourceGenerator.Smoke.csproj` (create), `tools/Cortex.Mediator.SourceGenerator.Smoke/Program.cs` (create), `docs/plans/2026-11-20-pr235-review-tests.md` (modify)
Depends on: Task 3
- [ ] Add an isolated net8 console consumer with existing mediator and generator analyzer project references, no new package versions, and assembly opt-in.
- [ ] Make generated DI/dispatch, representative pipelines/cancellation and all five message kinds fail visibly on wrong results; print one named success line only after all smoke assertions pass.
- [ ] Run managed, trimmed and win-x64 native publish/execution commands from Verification; record warnings and limits. Do not install prerequisites or redesign runtime APIs if blocked.
- [ ] Run Release build and full Cortex.Tests, account for every issue requirement and review finding, perform sequential review lenses, and record any confirmed/deferred findings.
Run: `dotnet run --project tools/Cortex.Mediator.SourceGenerator.Smoke/Cortex.Mediator.SourceGenerator.Smoke.csproj -c Release && dotnet build src/Cortex.Tests/Cortex.Tests.csproj -c Release && dotnet test src/Cortex.Tests/Cortex.Tests.csproj -c Release`
Expected: exit 0; smoke success line; Release build succeeds; nonzero test discovery and 0 failed. Trim/native publish and execution must also succeed before claiming those scenarios verified; otherwise this task is blocked with evidence, not marked done.
Commit: test(mediator): add generated consumer native smoke coverage
## Risks
- Baseline test/restore failures may prevent progress; never weaken assertions or checks.
- Native compiler/linker prerequisites or runtime pack downloads may be unavailable. Stop and report what unblocks validation; installation is not authorized.
- Generated runtime pipeline configuration uses reflection. A smoke success is not proof for all options, platforms or closed behavior types.
- New diagnostics and exact-type dispatch change consumer build outcomes; only reproduced defects receive fixes, with compatibility controls.
- Dynamic assembly tests must use unique names and share host dependencies; repeated runs must remain isolated.
- Code-reviewer delegation is disallowed by the platform. Inline lenses and QA help do not equal the originally proposed independent review/refutation panel.
## Progress
- 2026-11-20 Ruling: Review PR #235 — it is the sole open PR and explicitly implements issue #214.
- 2026-11-20 Ruling: Branch from the PR head, not master — corrections must apply to the reviewed code, leaving master intact.
- 2026-11-20 Ruling: Reuse executable Roslyn fixtures, add a separate console smoke — minimizes test wiring while preserving a normal analyzer/native consumer check; cost if wrong is test-host loading complexity.
- 2026-11-20 Cap: Independent Code Reviewer review/refutation/completeness panels dropped — all four delegations rejected by allowed delegation list before starting; lead will perform the lenses inline, with QA support if permitted.
- 2026-11-20 Review: static candidates only; no tests run yet and no production defect claimed reproduced.
- 2026-11-20 Task 1: repair 1/3 — baseline `dotnet test … --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator` → MSB4242 workload manifest mismatch (Emscripten.net9 10.0.105 < 10.0.114 required by mono.toolchain.net9) → retried with MSBuildEnableWorkloadResolver=false for that command only.
- 2026-11-20 Task 1: repair 2/3 — NETSDK1226 "Prune Package data not found .NETCoreApp 10.0" for net10.0 targets of Cortex.Mediator, Cortex.Streams, Cortex.Streams.Mediator, Cortex.Mediator.Behaviors.Transactional → retried with -p:AllowMissingPrunePackageData=true for that command only.
- 2026-11-20 Task 1: repair 3/3 — exit -532462766: NETSDK1060 reading obj/project.assets.json, MSB4018 FileNotFoundException 'System.Threading.ThreadPool, Version=10.0.0.0', MSB0001 internal MSBuild error.
- 2026-11-20 Task 1: BLOCKED — the local .NET SDK 10.0.403 toolchain fails before compiling any project; no test has run, no code changed — a repaired SDK/workload install (Visual Studio Installer repair, or reinstalling the .NET 10.0.403 SDK and `dotnet workload repair`) or a working build machine would unblock it.
- 2026-11-20 Task 1: unblocked — user repaired the SDK (now 10.0.404); baseline focused run → "Passed! - Failed: 0, Passed: 24".
- 2026-11-20 Ruling: fixture references = the test host's trusted platform assemblies minus Cortex.Tests.dll — Cortex.Tests carries its own generated marker attribute; cost if wrong: slower fixture compilation only.
- 2026-11-20 Task 1: complete — `dotnet test … --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator` → "Passed! - Failed: 0, Passed: 32" (8 new runtime tests).
- 2026-11-20 Task 2: red — 8 new tests failed for the predicted reasons: no CXMED001-004 for record/struct messages; partial PingHandler → false CXMED005 + CS8120, partial message → CXMED001 twice; CXMED005/006 at Location.None; no stream duplicate diagnostic; no diagnostic for a private nested handler; record handler not registered.
- 2026-11-20 Ruling: CXMED008 (inaccessible handler) is a Warning, not an Error — it matches CXMED001 for an unhandled command, which it effectively is; cost if wrong: a skipped handler fails at runtime instead of at build.
- 2026-11-20 Ruling: GeneratedMediatorEmitter (listed under Task 3) and the generator README diagnostics table changed in Task 2 — one switch case per message keeps CXMED005-007 the only error, and the table must list the two new IDs; cost if wrong: none.
- 2026-11-20 Task 2: complete — `dotnet test … --filter FullyQualifiedName~Cortex.Tests.Mediator.SourceGenerator` → "Total tests: 40, Passed: 40".
