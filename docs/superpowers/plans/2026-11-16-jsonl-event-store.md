# JSONL event store + store-family move — implementation plan (rev 3)

> **SUPERSEDED — lock files (2026-11-18, commit `7bff365`).** This plan is a historical record of how
> the work was done. Its steps tell the implementer to "commit its updated `packages.lock.json`" and
> state "CI uses locked restore"; **that policy was reversed after the work landed.** A
> `packages.lock.json` is now a LOCAL RECORD: every restore writes it, `.gitignore` keeps it out of the
> tree, it is never committed and never reviewed, and CI runs no locked restore at all (the resolved
> versions leave each run as the `resolved-versions` artifact). Where this plan and the current
> `Directory.Packages.props` / `ci.yml` disagree about lock files, the current files win.
>
> The lock-file instructions inside the steps are annotated in place below ("restore it, do not commit
> it") rather than rewritten, so the record of the original decision stays readable.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `PicoActor.EventStore.Jsonl` (a generic, AOT/source-generated JSONL event store) and move `InMemoryEventStore` into `PicoActor.EventStore.InMemory`, so the core package carries no concrete store.

**Architecture:** Contracts stay in `PicoActor.Abs` (Option A). Two store packages reference it; the JSONL store implements `IEventStore`, `IEventLogReader`, `IEventStoreSnapshot`, `IEventStoreEnumerator` and uses PicoJetson over a consumer-declared `[PicoDerivedType]` base. A Roslyn analyzer (`PicoActor.EventStore.Gen`) turns missing registrations into a compile-time diagnostic. One x release carries the move + the new package.

**Tech Stack:** C# / net10.0, TUnit, PicoJetson, PicoLog.Abs.

**Spec:** `docs/superpowers/specs/2026-11-16-jsonl-event-store-design.md` (rev 5)

## Global Constraints

- Contracts are not modified except the documented `GetVersionAsync` doc relaxation (Task 6). `IEventStore` keeps its three members.
- The family is **three** packages: `.InMemory`, `.Jsonl`, `.Gen` (embedded). No `EventStore.Abs`.
- `PicoActor` core compiles without a concrete store; the missing-store case fails at **resolve** time with `InvalidOperationException` naming `AddPicoActorInMemoryStore()`.
- Registration is `[PicoDerivedType]` on the base only; `[PicoSerializable]` on a derived type is not sufficient. `PicoActor.TestSupport` is a **registration consumer** (it declares `ConformanceEvent`), so it references PicoJetson.
- Append order: read-only witness (also for empty batches) → `expectedVersion` → torn-tail truncate → **one** `WriteAsync` → `FlushAsync(CancellationToken.None)` → update cache. A rejected append never changes content (a missing stream may be left as a zero-length file).
- Torn tail (unterminated final line) ignored; a **terminated** bad line throws; corruption is not repaired.
- Count/`End`/`GetVersionAsync` come from one byte scan (line count, no JSON validation).
- Build 0 warnings; `dotnet csharpier format` touched files before each commit; a project whose references changed only needs a `dotnet restore` — the `packages.lock.json` a restore writes is a local, gitignored record and is **not** committed (there is no locked restore in CI; see the Superseded note above).
- **Do not pre-refresh `api/*.public.txt`**; `scripts/release.ps1` refreshes it at release time.
- Test arithmetic: `PicoActor.Tests` **229 → 230** (Task 2b: +2 −1) **→ 213** (Task 2c moves 17 out); `PicoActor.EventStore.InMemory.Tests` **17 at Task 2c** (the base still has 12 cases) **→ 18** after Task 3a adds one conformance case; `PicoActor.Abs.Tests` **51** unchanged.

## Review Focus

1. Empty batch + stale `expectedVersion` throws and changes no content — Task 3b, `EmptyBatch_StaleVersion_Throws_NoContentChange`.
2. Torn tail is repaired by the next append and the new event is readable — Task 3b, `TornTail_IsRepairedByNextAppend`.
3. A terminated corrupt line throws on `LoadAsync` and the cursor, and is not repaired — Task 3b, `CorruptTerminatedLine_Throws`.
4. A snapshot rebuild deserializes only the tail (`DeserializedEventCount` delta == `N − v`) — Task 3c, `SnapshotRebuild_DeserializesOnlyTheTail`.
5. A consumer that forgot `[PicoDerivedType]` gets `PICOSTORE001` — Task 4, `Analyzer_FlagsUnregisteredEvent`.

Documented, not testable in-repo: PicoAgent adoption (file rename, second codec, snapshot rewrite, ledger/archive, fsync) — spec §Intent.

## File Structure

- `tests/PicoActor.TestSupport/` — `EventLogReaderConformanceBase` (TUnit `[Test]` methods), `ConformanceEvent` (`[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]`), `SSnapshotAdded`; `namespace PicoActor.TestSupport`; references `PicoActor.Abs` + **PicoJetson** + TUnit; `IsTestProject=false`, no `OutputType=Exe`.
- `tests/PicoActor.EventStore.InMemory.Tests/` — the 17 moved InMemory store tests (+ the base's blank-line case = 18).
- `src/PicoActor.EventStore.InMemory/` — `InMemoryEventStore` (moved) + `AddPicoActorInMemoryStore()`.
- `src/PicoActor.EventStore.Jsonl/` — store, options, cursor, snapshot file, locks.
- `src/PicoActor.EventStore.Gen/` — analyzer (embedded into Jsonl).
- `tests/PicoActor.EventStore.Jsonl.Tests/`, `tests/PicoActor.EventStore.Gen.Tests/`.
- `PicoActor`/`PicoActor.Abs`: DI resolve-time throw, Description/doc updates.
- Repo: `PicoActor.slnx`, `ci.yml`, `release.yml`, `Directory.Packages.props`, `scripts/verify-package.sh`, `samples/`.

---

### Task 0: CPM baseline

**Files:** `Directory.Packages.props`

- [ ] **Step 1:** add `<PackageVersion Include="PicoJetson" Version="2026.13.*" />` (first-party float band) and `<PackageVersion Include="PicoActor.EventStore.InMemory" Version="$(Version)" />`, `…Jsonl…` — before any project references them.
- [ ] **Step 2: Commit** `git add Directory.Packages.props && git commit -m "chore(deps): pin PicoJetson and the new store packages"`.

---

### Task 1: Test support (conformance base + polymorphic seed)

**Files:** create `tests/PicoActor.TestSupport/`; modify `tests/PicoActor.Tests/PicoActor.Tests.csproj`, `tests/PicoActor.Tests/GlobalUsings.cs`; remove the local base/seed from `EventLogReaderConformanceTests.cs` and `FixSSnapshotTests.cs`.

**Interfaces:** Produces `PicoActor.TestSupport.EventLogReaderConformanceBase`, `ConformanceEvent`, `SSnapshotAdded` (public).

- [ ] **Step 1:** project: `net10.0`, `IsTestProject=false`, `PackageReference` TUnit **and PicoJetson** with `PrivateAssets="none"` (spelled out: the default `contentfiles;analyzers;build` cuts the generator and `build/PicoJetson.targets` off — this project is a registration consumer, so it needs both), `ProjectReference` to `PicoActor.Abs` only.
- [ ] **Step 2:** move the base (namespace `PicoActor.TestSupport`); seed: `public abstract record ConformanceEvent : IDomainEvent` with `[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]`; `public sealed record SSnapshotAdded(int Delta) : ConformanceEvent` (name kept — `RebuildReaderPathTests.cs:280` uses it).
- [ ] **Step 3:** `PicoActor.Tests` references TestSupport; add `global using PicoActor.TestSupport;`; delete the local declarations. Run `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress` → **229/229**; `dotnet restore` the project so its local `packages.lock.json` record picks up the new transitive PicoJetson (the file is gitignored: restore it, do not commit it).
- [ ] **Step 4: Commit** `git add tests/PicoActor.TestSupport tests/PicoActor.Tests && git commit -m "test: share the conformance base and a polymorphic seed event"`.

---

### Task 2a: Create `PicoActor.EventStore.InMemory` and move the store

**Files:** create `src/PicoActor.EventStore.InMemory/` (csproj, `GlobalUsings.cs`, moved `InMemoryEventStore.cs`); modify `PicoActor.slnx`, `tests/PicoActor.Tests` (ProjectReference + `global using PicoActor.EventStore.InMemory;`), `samples/PicoActor.Sample`.

**Interfaces:** Produces `InMemoryEventStore` (same behaviour, new namespace).

- [ ] **Step 1:** csproj: `PackageId=PicoActor.EventStore.InMemory`, `IsPackable=true`, `PackageReadmeFile`, `..\..\README.md`, tags `PicoHex;PicoActor;AOT;In-memory event store`, dual `UseProjectReferences` item groups for `PicoActor.Abs` (no `PicoLog.Abs`/`PicoDI.Abs`: the file has no logger; DI comes from Abs).
- [ ] **Step 2:** `git mv src/PicoActor/InMemoryEventStore.cs src/PicoActor.EventStore.InMemory/`; namespace update; add `GlobalUsings.cs` (`global using System.Collections.Concurrent;` + `global using PicoActor.Abs;` — `ConcurrentDictionary` is not in implicit usings); no behaviour change.
- [ ] **Step 3:** add the package to `PicoActor.Tests` + `samples`; `dotnet restore` both (their `packages.lock.json` records are gitignored local files, not commit content); `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress` → **229/229**.
- [ ] **Step 4: Commit** (`git add` the new directory explicitly; the lock files stay untracked and gitignored): `git commit -m "feat(store): move the in-memory store into its own package"`.

---

### Task 2b: Core DI becomes store-free (TDD)

**Files:** `src/PicoActor/PicoActorDiExtensions.cs`, `src/PicoActor/ActorConfig.cs`, `src/PicoActor/PicoActor.csproj` (Description), `src/PicoActor.EventStore.InMemory/InMemoryStoreDiExtensions.cs`; tests `tests/PicoActor.Tests/PicoActorDiExtensionsTests.cs`, `ActorConfigTests.cs`.

**Interfaces:** Produces `public static ISvcContainer AddPicoActorInMemoryStore(this ISvcContainer container)`.

- [ ] **Step 1: Write the failing tests**:

```csharp
[Test] AddPicoActor_WithoutStore_ResolveThrows_InvalidOperation_NamingInMemoryExtension
    // AddPicoActor(); Build(); GetService<IActorSystem>() throws InvalidOperationException with a message
    // containing "AddPicoActorInMemoryStore" and "PicoActor.EventStore.InMemory"
[Test] AddPicoActorInMemoryStore_ThenAddPicoActor_ResolvesInMemoryStore
    // registers the store first; resolves IActorSystem whose store is an InMemoryEventStore
```

Extend the existing `ActorConfigTests.AddPicoActor_WithUnsupportedStoreType_Throws` with the exact spec message (do not add a duplicate test). Delete `AddPicoActor_NoArgs_RegistersDefaultInMemoryStore` (premise gone).

- [ ] **Step 2: Run** → the two new tests fail.
- [ ] **Step 3: Implement:** `AddPicoActorInMemoryStore()` in the InMemory package; core factory: resolve `IEventStore`, throw `InvalidOperationException("No IEventStore is registered. Call AddPicoActorInMemoryStore() (PicoActor.EventStore.InMemory) or register your own store.")`; `AddPicoActor(container, ActorConfig?)` keeps the switch, `"InMemory"` = must already be registered, unknown → `NotSupportedException("Supported: whatever IEventStore is already registered (e.g. AddPicoActorInMemoryStore()); other stores are constructed explicitly.")`; drop the "provides InMemoryEventStore" Description/doc claims.
- [ ] **Step 4:** the default-store cases in `ActorConfigTests` (3 of 4), `PicoActorDiExtensionsTests.AddPicoActor_WithPublisher_WiresEventOutflow`, `CommandSenderTests.AddPicoActor_RegistersSender_ResolvableFromChildScope`, and `MediatorIntegrationTests` (~10) register the InMemory store first. Run → green (**230**).
- [ ] **Step 5: Commit** `git add src tests && git commit -m "feat(di): make the core store-free and move the in-memory default into its package"`.

---

### Task 2c: Move the 17 InMemory store tests

**Files:** create `tests/PicoActor.EventStore.InMemory.Tests/`; `git mv` `EventLogReaderConformanceTests.cs` (12), `InMemoryEventStoreEnumeratorTests.cs` (4), `InMemoryEventStoreConcurrencyTests.cs` (1) from `PicoActor.Tests`.

- [ ] **Step 1:** new test project (TUnit, Exe), references `PicoActor.EventStore.InMemory` + `PicoActor.TestSupport`; `global using PicoActor.TestSupport;`.
- [ ] **Step 2:** move the files; run `dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress` → **213**; `dotnet test --project tests/PicoActor.EventStore.InMemory.Tests/PicoActor.EventStore.InMemory.Tests.csproj --no-progress` → **17**.
- [ ] **Step 3: Commit** `git add tests/PicoActor.EventStore.InMemory.Tests && git add -u && git commit -m "test(store): move the in-memory store tests into their own project"` (the `git add -u` records the removed files).

---

### Task 3a: JSONL scanning, count and cursor (conformance first)

**Files:** create `src/PicoActor.EventStore.Jsonl/` (csproj, `GlobalUsings.cs`, `JsonlEventStore.cs` skeleton, `JsonlCursor.cs`, `JsonlLocks.cs`); tests `tests/PicoActor.EventStore.Jsonl.Tests/JsonlConformanceTests.cs` + `GlobalUsings.cs`.

**Interfaces:** Produces `JsonlEventStore<TEventBase>` (4 interfaces) with internal `long DeserializedEventCount`, `long TornTailLines`, `long SkippedFileNames`, `int LockEntryCount` (IVT).

- [ ] **Step 1: csproj** — copy the `PicoActor.csproj` packaging skeleton: `PackageId=PicoActor.EventStore.Jsonl`, README `..\..\README.md`, tags, dual `UseProjectReferences` groups (Abs) with the repo's `PrivateAssets=Build` shape, `PackageReference PicoJetson` (**`PrivateAssets="none"`**; the default `contentfiles;analyzers;build` writes `exclude="Build,Analyzers"` and cuts the generator/targets off the consumer), `PicoLog.Abs` (**`PrivateAssets="Build"`**), `<InternalsVisibleTo Include="PicoActor.EventStore.Jsonl.Tests" />`.
- [ ] **Step 2: test project** — `JsonlConformanceTests : EventLogReaderConformanceBase` over a temp dir; `GlobalUsings.cs` with `global using PicoActor.TestSupport;` and `global using PicoActor.EventStore.Jsonl;`. Run `dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj --no-progress -- --treenode-filter "/*/*/JsonlConformanceTests/*"` → compile error.
- [ ] **Step 3: implement** path `<id:D>.jsonl`, byte count scan (blank = all bytes in {0x20,0x09,0x0D}), `End`, `OpenAsync` positioning by counted events, `cap = fromVersion > End ? 0 : End − fromVersion + 1`, `GetVersionAsync` (same scan), `PeekFirstAsync` (null for empty/torn/corrupt first line), `ListAggregateIdsAsync` (skip foreign names/empty; corrupt first line → no id), `LoadAsync` (eager List).
- [ ] **Step 4:** add one conformance case to the base (`PicoActor.TestSupport`): `BlankLines_NeitherCountedNorPositionedOver` (its counterpart `OpenPastVertex_DeserializesNothing` lives in Task 3c because it asserts the store counter). Run the same filter → 13/13.
- [ ] **Step 5: Commit** `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): scan, count and cursor the JSONL stream"`.

---

### Task 3b: Append protocol

**Files:** `JsonlEventStore.cs`; tests `JsonlEventStoreTests.cs`.

- [ ] **Step 1: failing tests**:

```csharp
AppendLoad_RoundTrip_WithPolymorphicBase              // Version == N, values match
EmptyBatch_StaleVersion_Throws_NoContentChange        // ConcurrencyException; content/count unchanged
EmptyBatch_FreshVersion_CreatesNothing                // store dir unchanged; count unchanged
NonTEventBase_Throws_ArgumentException                // message names TEventBase and the offender
TornTail_IsRepairedByNextAppend                       // half line ignored (count N), next append truncates, loads N+1
CorruptTerminatedLine_Throws                          // middle and final
PowerLossGarbage_IsCorruption_NotRepaired             // terminated garbage line -> throws, no repair
ExternalLengthChange_Throws_Concurrency               // truncate behind the store's back -> ConcurrencyException
UnregisteredDerived_ThrowsWrappedFormatException      // derived event without [PicoDerivedType] -> wrapped FormatException
UnregisteredBase_ThrowsWrappedInvalidOperationException // TEventBase not generated -> wrapped InvalidOperationException
```

- [ ] **Step 2: Run** `dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj --no-progress -- --treenode-filter "/*/*/JsonlEventStoreTests/*"` → all fail.
- [ ] **Step 3: implement** the 8-step append order (read-only witness, version, truncate, one `WriteAsync`, `FlushAsync(CancellationToken.None)`, cache update; failed write invalidates).
- [ ] **Step 4: Run** the same filter → 10/10; `dotnet csharpier format` the touched files; Commit `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): implement the JSONL append protocol"`.

---

### Task 3c: Snapshots

**Files:** `JsonlSnapshotFile.cs`, `JsonlEventStore.cs`; tests `JsonlSnapshotTests.cs`.

- [ ] **Step 1: failing tests**:

```csharp
Snapshot_RoundTrip                            // save + load, same version/payload
Snapshot_VersionAboveCount_ReturnedUnchanged  // store does not validate against the stream
Snapshot_ReadReturnsFreshArray                // two reads do not share the array
SnapshotHeader_IsLittleEndian                 // bytes 4..12 LE u64 version; 12..16 LE u32 length
Snapshot_InvalidMagicOrTruncated_ReturnsNull  // missing/bad magic/short header/version 0/length mismatch -> null
Snapshot_AtomicReplace_RemovesTempOnFailure   // a failing write leaves no .tmp
SnapshotRebuild_DeserializesOnlyTheTail       // DeserializedEventCount delta == N - v
OpenPastVertex_DeserializesNothing            // fromVersion past the end -> DeserializedEventCount unchanged
```

- [ ] **Step 2: Run** `… --treenode-filter "/*/*/JsonlSnapshotTests/*"` → fail.
- [ ] **Step 3: implement** `PASJ` + LE + temp/flush/rename + null rules + `SaveSnapshotAsync` copies before completing.
- [ ] **Step 4: Run** → 8/8; `dotnet csharpier format src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): add atomic JSONL snapshot sidecars"`.

---

### Task 3d: Locks, counters, logger

**Files:** `JsonlLocks.cs`, `JsonlEventStore.cs`; tests `JsonlDiagnosticsTests.cs`.

- [ ] **Step 1: failing tests**:

```csharp
LockEntryCount_StaysBounded            // many ids -> <= MaxAggregateLocks
Ctor_InvalidArgs_Throw                 // null/empty root -> ArgumentException; MaxAggregateLocks <= 0 -> ArgumentOutOfRangeException
TornTail_ReportsWarningAndCounter      // Options.Logger sees the warning; TornTailLines == 1
SkippedFileNames_ForForeignFiles       // non-Guid names skipped and counted
CorruptFirstLine_YieldsNoId_NoThrow    // ListAggregateIdsAsync skips it (PeekFirst -> null), never aborts
```

- [ ] **Step 2: Run** `… --treenode-filter "/*/*/JsonlDiagnosticsTests/*"` → fail.
- [ ] **Step 3: implement** the LRU entry (gate+count+witness, evict only idle, release-before-idle), counters, `Options.Logger`.
- [ ] **Step 4: Run** → 5/5; `dotnet csharpier format src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): bound the lock table and expose diagnostics"`.

---

### Task 4: `PICOSTORE001` analyzer

**Step 0 (project):** `tests/PicoActor.EventStore.Gen.Tests/` — `OutputType=Exe`, TUnit, `Microsoft.CodeAnalysis.CSharp` (`PrivateAssets="all"`), `ProjectReference` to `PicoActor.Abs` (the test compilation needs `IDomainEvent` metadata) and a `ProjectReference` to `src/PicoActor.EventStore.Gen` with `OutputItemType="Analyzer"` (the `ActorSubscriberGeneratorOutputTests` pattern instantiates the generator, so the assembly must be referenced — do **not** set `ReferenceOutputAssembly="false"`).

**Files:** `src/PicoActor.EventStore.Gen/` (copy `PicoActor.Gen.csproj`: `TreatAsLocalProperty="PublishAot"`, `PublishAot=false`, `TargetFramework=netstandard2.0`, `IsPackable=false`, `EnforceExtendedAnalyzerRules=true`, `IsRoslynComponent=true`, `NoWarn RS2008;RS1041;NU5128`, `Microsoft.CodeAnalysis.CSharp PrivateAssets=all`, `GenerateDependencyFile=false`); Jsonl csproj gets the two-item pair.

**Interfaces:** `DiagnosticDescriptor` `PICOSTORE001` (default `Info`).

- [ ] **Step 1: failing tests** (in-process compilation):

```csharp
Analyzer_FlagsUnregisteredEvent          // class E : IDomainEvent, no base attribute -> exactly one PICOSTORE001
Analyzer_Silent_WhenBaseRegistersDerived // base with [PicoDerivedType] -> zero
Analyzer_Reports_PicoSerializableOnlyDerived // derived with only [PicoSerializable] -> one
```

- [ ] **Step 2: implement** — **no ProjectReference to Abs** (netstandard2.0): match `global::PicoActor.Abs.IDomainEvent` and the attribute FQN `global::PicoSerDe.PicoDerivedTypeAttribute` (it lives in `PicoSerDe.Core`, transitive via PicoJetson) by metadata name; predicate: the type is not listed by any `[PicoDerivedType]` on a base in the compilation/references.
- [ ] **Step 3:** wire the two-item pair into the Jsonl csproj; run `dotnet test --project tests/PicoActor.EventStore.Gen.Tests/PicoActor.EventStore.Gen.Tests.csproj --no-progress -- --treenode-filter "/*/*/AnalyzerTests/*"`; `dotnet build src/PicoActor.EventStore.Jsonl/PicoActor.EventStore.Jsonl.csproj -c Release` (the slnx is wired in Task 5, so build the project directly) → 0 warnings; `dotnet csharpier format src/PicoActor.EventStore.Gen tests/PicoActor.EventStore.Gen.Tests`; Commit `git add src/PicoActor.EventStore.Gen tests/PicoActor.EventStore.Gen.Tests && git commit -m "feat(store): flag unregistered domain events at compile time"`.

---

### Task 5: Repo surface

**Files:** `PicoActor.slnx`, `ci.yml`, `release.yml`, `scripts/verify-package.sh`, `samples/`.

- [ ] **Step 1:** slnx adds **seven** projects: `src/PicoActor.EventStore.InMemory`, `src/PicoActor.EventStore.Jsonl`, `src/PicoActor.EventStore.Gen`, `tests/PicoActor.TestSupport`, `tests/PicoActor.EventStore.InMemory.Tests`, `tests/PicoActor.EventStore.Jsonl.Tests`, `tests/PicoActor.EventStore.Gen.Tests`.
- [ ] **Step 2:** `ci.yml` adds `dotnet test --project` steps for the InMemory/Jsonl/Gen test projects.
- [ ] **Step 3:** `release.yml` pack order: `PicoActor.Abs` → `PicoActor.EventStore.InMemory` → `PicoActor.EventStore.Jsonl` → `PicoActor`, each with `-p:UseProjectReferences=false -p:RestoreAdditionalProjectSources=${{ github.workspace }}/artifacts/nupkg -o artifacts/nupkg`.
- [ ] **Step 4:** `verify-package.sh` (four edits):
  - pack section adds `src/PicoActor.EventStore.InMemory/PicoActor.EventStore.InMemory.csproj`; the nuspec print loop adds that package; the clean loop becomes `for id in picoactor picoactor.abs picoactor.eventstore.inmemory`.
  - both **positive** probes keep `PicoActor` **and** add `PicoActor.EventStore.InMemory`; their sources add `using PicoActor.EventStore.InMemory;` and keep `new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() })` (`ActorSystem` lives in `PicoActor`; do not switch to the DI form).
  - new **negative** probe (references `PicoActor` + `PicoDI`; `SvcContainer` lives in `PicoDI`, `GetService<T>` is an `ISvcScope` extension):
    ```csharp
    using PicoActor.Abs;
    using PicoDI;
    var container = new SvcContainer(autoConfigureFromGenerator: false);
    container.AddPicoActor();
    container.Build();
    await using var scope = container.CreateScope();
    _ = scope.GetService<IActorSystem>();   // must throw
    ```
    run it expecting a non-zero exit whose output contains `AddPicoActorInMemoryStore`.
- [ ] **Step 5:** sample: references both store packages; add a `[PicoDerivedType]` event base for a minimal counter; JSONL section writes to a temp dir, asserts append → rebuild `Version == N` → a **new** store instance reads `N` → no sentinel file → deletes the dir; `=== Done ===` last.
- [ ] **Step 6:** full build + all tests + csharpier; Commit `git add .github scripts samples PicoActor.slnx && git commit -m "build: wire the store packages into the solution, CI and release"`.

---

### Task 6: Docs and release 2026.9.0

**Files:** `README.md` + 9 translations (10 files), `picohex-actor` skill, `docs/release-notes/v2026.9.0.md`, plus the I7 doc edits (`IEventLogReader.GetVersionAsync` XML; layering spec `§Semantics` items 1 and 2; `§Prerequisite` P1 note) and the stale comments `src/PicoActor/ActorSystemOptions.cs:53`, `src/PicoActor/ActorSystem.cs:1437`.

- [ ] **Step 1:** README section: package, registration form (`[PicoDerivedType]` on the base only), torn-tail/corruption policy, cursor/tail-only rebuild, `FlushToDisk`; InMemory DI extension. Skill: packages + durable-store subsection. Release note: x bump, downstream migration (package refs + usings + `AddPicoActorInMemoryStore()`), float band.
- [ ] **Step 2:** apply the I7 doc edits and the two stale-comment updates.
- [ ] **Step 3:** verification:

```bash
dotnet build PicoActor.slnx --no-restore -c Release
dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release --no-build --no-progress   # 213
dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release --no-build --no-progress # 51
dotnet test --project tests/PicoActor.EventStore.InMemory.Tests/PicoActor.EventStore.InMemory.Tests.csproj -c Release --no-build --no-progress # 18
dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj -c Release --no-build --no-progress
dotnet csharpier check .
```
- [ ] **Step 4:** api delta to `artifacts/` (do not pre-refresh):

```bash
dotnet run --project scripts/api-surface -- dump src/PicoActor.Abs/bin/Release/net10.0/PicoActor.Abs.dll --out artifacts/api-abs.txt
dotnet run --project scripts/api-surface -- diff api/PicoActor.Abs.public.txt artifacts/api-abs.txt   # 0 changes
dotnet run --project scripts/api-surface -- dump src/PicoActor/bin/Release/net10.0/PicoActor.dll --out artifacts/api-picoactor.txt
dotnet run --project scripts/api-surface -- diff api/PicoActor.public.txt artifacts/api-picoactor.txt # InMemoryEventStore rows leave
```
- [ ] **Step 5:** commit docs (`git add README*.md docs/release-notes/v2026.9.0.md src/PicoActor.Abs/IEventLogReader.cs docs/superpowers/specs/... && git commit -m "docs: document the JSONL store and the store-family move"`), then `./scripts/release.ps1 -DryRun` → **API changed → v2026.9.0**; `./scripts/release.ps1 -Push` (refreshes `api/`, commits, tags, pushes); watch Release + CI; verify the nupkg README.

## Self-Review

1. **Spec coverage:** §Package → Tasks 0/1/2a/2b/2c/5; §Serialization + §Analyzer → Tasks 3a–3d/4; §Semantics → 3a–3d; §Repo surface → 5; §Docs/versioning → 6. Out-of-repo (PicoAgent) stated, not tasked.
2. **Step scan:** each step is one action; every test step names its test and assertion; every implementation step carries the spec-fixed algorithm; every task has an explicit `git add` + commit.
3. **Type consistency:** `JsonlEventStore<TEventBase>`, `JsonlEventStoreOptions`, `ConformanceEvent`, `AddPicoActorInMemoryStore(this ISvcContainer container)`, `PICOSTORE001` — consistent across tasks.
4. **Review Focus:** five items mapped to named tests; PicoAgent documented as untestable in-repo.
5. **Proportion:** plan ≈ 290 lines vs spec ≈ 350; no implementation bodies beyond the spec-fixed algorithm.
