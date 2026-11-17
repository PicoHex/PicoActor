# `FileNameFormat` implementation plan (rev 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `JsonlEventStoreOptions.FileNameFormat` (`IdOnly` default, `TypePrefix` `{type}-{id}.jsonl`), so a directory of `{type}-{id}.jsonl` streams is enumerated by filename (zero opens) instead of by peeking every first line.

**Architecture:** A naming resolver maps an aggregate id to a path. `IdOnly` derives `<id:D>.jsonl`; `TypePrefix` resolves through an `id → type` map (one filename scan at construction, extended on first append from `events[0].GetType().Name`, and completed by a root pass on a miss). Enumeration classifies every directory entry into five buckets; a whole-directory misconfiguration throws instead of enumerating nothing. Snapshots stay `<id>.snap`. The work is split into four reviewable tasks: surface+parsing, TypePrefix resolution, classification+guard, lazy pass.

**Tech Stack:** C# / net10.0, TUnit, PicoLog.Abs.

**Spec:** `docs/superpowers/specs/2026-11-17-jsonl-filename-format-design.md` (rev 3, APPROVED)

## Global Constraints

- `IdOnly` is byte-identical to v2026.9.0: file names, enumeration results, counts. No directory scan at construction; appends to **existing** streams do not scan. Exactly **one** lazy root pass per instance, at the first observation of a **missing** stream (any member; it latches), and only then do `DirectoryScans == 1`.
- `TypePrefix` file name = `{events[0].GetType().Name}-{id:D}.jsonl` (backtick arity kept; never `FullName`, never stripped). Prefix fixed at creation.
- Parsing: `name.EndsWith(StreamSuffix, Ordinal)` **first**, then `stem = name[..^suffix]`; the stem is split at its **first** `-` (non-empty type prefix + `D` GUID); `Guid.TryParseExact(…, "D")`. Never `LastIndexOf('-')`, never a bare `name[..^N]` on a short foreign name.
- **Symmetric format detection**: under `TypePrefix`, a bare `<guid>.jsonl` name (the `IdOnly`
  rule) is **`OtherFormat`**, not `Foreign` — pointing a `TypePrefix` store at an `IdOnly`
  directory is a whole-directory misconfiguration and throws, never a silent empty enumeration.
- `ConfiguredMatch` additionally requires `FileInfo.Length > 0` (a zero-length configured file
  is not a stream, matching `IdOnly`); the length comes from `DirectoryInfo.EnumerateFiles`
  metadata, so `FilesOpened` stays 0.
- `ConfiguredOtherType` is produced by the **enumeration side** (`ConfiguredMatch` +
  `type != firstEventType`), not by `Classify` itself.
- Classification is one helper returning a kind:
  `internal enum StreamNameKind { ConfiguredMatch, ConfiguredOtherType, OtherFormat, UnparseableId, Foreign }` and
  `internal static StreamNameKind Classify(string fileName, FileNameFormat configured, out string type, out Guid id)`.
  Buckets: **ConfiguredMatch** (returned for the queried type), **ConfiguredOtherType** (not counted, no log), **OtherFormat** (`ForeignFormatFiles`, one Warning per scan), **UnparseableId** (`SkippedFileNames`), **Foreign** (any other directory entry, `SkippedFileNames`, existing behaviour).
- `DirectoryScans` increments **before** classification (so a throwing scan still counts). After a whole-directory misconfiguration throws, the instance latches the failure and **rethrows on the next observation** (never silently continues).
- Whole-directory misconfiguration (`configured == 0 && other > 0`) throws `InvalidOperationException` naming both formats and counts, from **any** scan (enumeration, constructor, lazy pass).
- Duplicate guard: `new FileInfo(path) is { Exists: true, Length: > 0 }` (never `File.Exists`), `TypePrefix` checks `<id>.jsonl` directly, `IdOnly` only after a scan observed `ForeignFormatFiles > 0`. Same id with several prefixes throws listing the candidates.
- `FilesOpened` increments **only in `OpenRead`** (never in the append's write handle); every test asserts **deltas**.
- Snapshots: `<id:D>.snap` in both formats, `PASJ` unchanged. `SaveSnapshotAsync`/`LoadSnapshotAsync` work for an id with **no stream**.
- Build 0 warnings; `dotnet csharpier format` before each commit; explicit `git add` (no `-am`). Do not pre-refresh `api/`.
- Test baselines: `PicoActor.Tests` **214**, `PicoActor.Abs.Tests` **51**, `PicoActor.EventStore.InMemory.Tests` **19**, `PicoActor.EventStore.Jsonl.Tests` **44** (+ new), `PicoActor.EventStore.Gen.Tests` **3**.

## Review Focus

1. `TypePrefix` enumerates the right ids with zero opens — Task 1b, `G2_TypePrefix_EnumeratesByFileName_WithZeroOpens`.
2. An `IdOnly` store pointed at a `{type}-{id}` directory never silently enumerates nothing — Task 1d, `G8_NoEnumerationHost_LazyPass_ThrowsOnMisconfiguration`.
3. A zero-length file in the other format never blocks an append — Task 1c, `G5b_OtherFormatZeroLength_AppendSucceeds`.
4. A short foreign name (`x.txt`) does not crash the scan — Task 1c, `G5c_ShortForeignName_CountedSkipped_NoThrow`.
5. An event type renamed after creation is invisible to `TypePrefix` and visible to `IdOnly` — Task 1c, `G7_TypeRename_IsFormatDependent`.

Documented, not testable in CI: the measured 4,600-file timings (opt-in harness; numbers go into the release notes).

## File Structure

- `src/PicoActor.EventStore.Jsonl/FileNameFormat.cs` (new enum)
- `src/PicoActor.EventStore.Jsonl/JsonlEventStoreOptions.cs` (property)
- `src/PicoActor.EventStore.Jsonl/JsonlNames.cs` (new: `Classify`, `TrySplitTypePrefix`, `TryParseIdOnly`)
- `src/PicoActor.EventStore.Jsonl/JsonlEventStore.cs` (map, resolver, scans, guard, counters, XML doc, `SkipFileName` text)
- `tests/PicoActor.EventStore.Jsonl.Tests/FileNameFormatTests.cs` (G1a–G9 + parser matrix)
- `tests/PicoActor.EventStore.Jsonl.Tests/Performance/FileNameFormatPerf.cs` (opt-in, `[Test, Explicit]`)
- `docs/release-notes/v2026.10.0.md`, `README.md` + 9 translations, `picohex-actor` skill,
  `docs/superpowers/specs/2026-11-16-jsonl-event-store-design.md` (rev-5 delta)

---

### Task 1a: Surface, parser and counters (no behaviour change)

**Files:** create `FileNameFormat.cs`, `JsonlNames.cs`; modify `JsonlEventStoreOptions.cs`, `JsonlEventStore.cs`; tests `FileNameFormatTests.cs`.

**Interfaces:**
- Produces `FileNameFormat`, `JsonlEventStoreOptions.FileNameFormat` (undefined value → `ArgumentOutOfRangeException(nameof(FileNameFormat))`), `StreamNameKind` + `Classify`, `IdOnlyPathFor(Guid)`, and the counters `FilesOpened` (in `OpenRead` only), `DirectoryScans`, `ForeignFormatFiles`.

- [ ] **Step 1: failing tests**:

```csharp
Parser_ConfiguredMatch_TypePrefix            // "SSnapshotAdded-<guid>.jsonl" -> ConfiguredMatch + type/id
Parser_OtherFormat_UnderIdOnly               // same name with configured=IdOnly -> OtherFormat
Parser_BareGuid_UnderTypePrefix              // "<guid>.jsonl" with configured=TypePrefix -> OtherFormat (symmetric detection)
Parser_ShortForeignName_NoThrow              // "x.txt" -> Foreign, no exception (EndsWith guard first)
Parser_Readme_IsForeign                      // "README.md" -> Foreign
Parser_UnparseableId                         // "SSnapshotAdded-notaguid.jsonl" -> UnparseableId
UndefinedFormat_Throws                       // new JsonlEventStoreOptions { FileNameFormat = (FileNameFormat)42 } -> AOORE with ParamName
G1a_IdOnly_NoScanAtConstruction_And_AppendToExisting // pre-write one <id>.jsonl; construct -> DirectoryScans == 0; append to it -> still 0; names/counts unchanged
```

- [ ] **Step 2: run** `dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj --no-progress -- --treenode-filter "/*/*/FileNameFormatTests/*"` → compile error.

- [ ] **Step 3: implement** the enum, the option + validation, `JsonlNames.Classify` (EndsWith guard → first-`-` split → `TryParseExact`), `IdOnlyPathFor`, and the three counters at their defined increments. No behaviour change for `IdOnly`.

- [ ] **Step 4: run** the filter → 8/8; full Jsonl suite → **52**; `dotnet csharpier check .`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): add the FileNameFormat surface and the stream-name classifier"`.

---

### Task 1b: TypePrefix resolution and creation

**Files:** `src/PicoActor.EventStore.Jsonl/JsonlEventStore.cs`, `src/PicoActor.EventStore.Jsonl/JsonlNames.cs`; `tests/PicoActor.EventStore.Jsonl.Tests/FileNameFormatTests.cs`.

**Interfaces:**
- Produces `internal string? PathFor(Guid id)` (null = no stream in this format; its miss branch performs the root pass), `internal string PathForNew(Guid id, string typeName)`; the construction scan (missing root → empty map, no throw, no directory creation).

- [ ] **Step 1: failing tests**:

```csharp
G2_TypePrefix_EnumeratesByFileName_WithZeroOpens
    // dirA (IdOnly): 3 streams -> ListAggregateIds -> FilesOpened delta == 3, ids set == expected
    // dirB (TypePrefix): 3 streams -> same call -> FilesOpened delta == 0, ids set == expected (empty set fails)
    // and the on-disk names in dirB are "{type}-{id}.jsonl"
G3_BothFormats_SameReadSemantics
    // GetVersionAsync / OpenAsync(End-3) / PeekFirstAsync equal in both; `OpenAsync(Guid.NewGuid(), 0)` throws in both; plus the corrupt-first-line case:
    // TypePrefix returns the id (FilesOpened == 0), IdOnly does not (documented deviation)
G4_Snapshot_Works_WithoutStream_And_AcrossInstances
    // TypePrefix: SaveSnapshotAsync for an id with NO stream -> <id>.snap; a NEW store instance loads it; PASJ bytes unchanged
```

- [ ] **Step 2: run** → fail (map/resolver missing).
- [ ] **Step 3: implement** the `id → type` map, construction scan, root pass on miss (completing the map), `PathFor`/`PathForNew`, the **`ListAggregateIdsAsync` TypePrefix branch** (keep `Classify`'s `ConfiguredMatch`, `FilesOpened` stays 0, zero-length skipped via `FileInfo.Length`), and the six members' `path is null` branches (LoadAsync → [], GetVersionAsync → 0, OpenAsync → **`fromVersion == 0` still throws first**, then `End == 0`, PeekFirstAsync → null, LoadSnapshotAsync → null, EmptyAppend → 0/0).
- [ ] **Step 4: run** → G2/G3/G4 green; full suite → **55**; `dotnet csharpier check .`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): resolve TypePrefix stream paths"`.

---

### Task 1c: Five-bucket classification, diagnostics and the duplicate guard

**Files:** `src/PicoActor.EventStore.Jsonl/JsonlEventStore.cs` (extends `ListAggregateIdsAsync`, `AppendAsync` guard, `SkipFileName` text, XML doc); `tests/PicoActor.EventStore.Jsonl.Tests/FileNameFormatTests.cs`.

- [ ] **Step 1: failing tests**:

```csharp
G2b_Buckets_OtherType_And_UnparseableId   // in a TypePrefix dir: "OtherType-<guid>.jsonl" (skip, no log) and
                                          // "SSnapshotAdded-notaguid.jsonl" -> SkippedFileNames delta == 1, ForeignFormatFiles delta == 0
G5a_OtherFormatNonEmpty_AppendThrows      // configured stream exists; non-empty other-format file for the target id;
                                          // one enumeration sets ForeignFormatFiles > 0; append -> IOE naming both paths
G5b_OtherFormatZeroLength_AppendSucceeds  // same, file empty -> append succeeds; file bytes unchanged
G5c_ShortForeignName_CountedSkipped_NoThrow // "x.txt" in the dir -> skipped, no throw
G5d_ScanReportsOneWarning                 // one enumeration -> RecordingLogger contains "use the other naming format" once
G6_TypePrefixOnlyDirectory_WithIdOnlyOptions_Throws // whole-directory misconfiguration, message has both counts
G5e_DuplicatePrefixes_Throws              // "A-<id>.jsonl" and "B-<id>.jsonl" both exist -> append throws IOE listing both
G7_TypeRename_IsFormatDependent           // "RetiredType-<id>.jsonl" holding an SSnapshotAdded line:
                                          // IdOnly enumerates the id, TypePrefix does not; append still uses the old prefix
```

Each case uses a **fresh `TempRoot` and a fresh store**; before asserting the guard, ensure a configured-format stream exists and run **one enumeration** (that is what sets `ForeignFormatFiles > 0`). `G2b` asserts separately: the other-type name appears **nowhere** in `RecordingLogger`; the unparseable-id name appears **once**; `SkippedFileNames` delta == 1; `ForeignFormatFiles` delta == 0. `G9` asserts `DirectoryScans` unchanged and `<id>.jsonl` still absent.

- [ ] **Step 2: run** → fail.
- [ ] **Step 3: implement** the five-bucket scan (with `DirectoryScans` increment first, one Warning per scan for the other format; the throw message template names both formats and counts), the whole-directory throw, the duplicate guard (`FileInfo{Exists, Length>0}`, gated by a **monotonically increasing** `ForeignFormatFiles`), and the same-id-multiple-prefix throw. **Extends** the `ListAggregateIdsAsync` branch started in 1b. Rewrite the class XML doc (`<id:D>.jsonl` is the default layout) and the `SkipFileName` message (must not claim a stream is always `<guid>.jsonl`).
- [ ] **Step 4: run** → green; full suite → **62**; `dotnet csharpier check .`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): classify stream files and guard mixed directories"`.

---

### Task 1d: Lazy pass, latch and the empty-batch order

**Files:** `src/PicoActor.EventStore.Jsonl/JsonlEventStore.cs`; `tests/PicoActor.EventStore.Jsonl.Tests/FileNameFormatTests.cs`.

- [ ] **Step 1: failing tests**:

```csharp
G8_NoEnumerationHost_LazyPass_ThrowsOnMisconfiguration
    // IdOnly store over a TypePrefix-only dir; no enumeration; first LoadAsync/append -> one lazy pass,
    // DirectoryScans == 1, throws IOE; a second observation rethrows (latched) and DirectoryScans stays 1
G9_TypePrefix_EmptyBatch_ValidatesVersion_AndCreatesNothing
    // existing stream with 3 events; AppendAsync(id, 0, []) -> ConcurrencyException; no file created;
    // AppendAsync(id, 3, []) -> no-op, no new file, no duplicate-guard call
```

- [ ] **Step 2: run** → fail.
- [ ] **Step 3: implement** the lazy pass (first missing-stream observation, any member, latched; increments `DirectoryScans` before classifying; remembers a misconfiguration and rethrows; TypePrefix's construction/miss passes also count, so the "== 1" claim applies to a store whose only scan is the lazy one), and the append order: acquire the per-aggregate gate → `events.Count == 0` → resolve path (map/pass) → version check → return (still inside the gate); non-empty → `type = events[0].GetType().Name` → path → gate → witness → version → truncate → one `WriteAsync` → `FlushAsync(None)` → cache/map.
- [ ] **Step 4: run** → G8/G9 green; full suite → **64**; `dotnet csharpier check .`; `git add src/PicoActor.EventStore.Jsonl tests/PicoActor.EventStore.Jsonl.Tests && git commit -m "feat(store): run one lazy scan and validate empty batches"`.

---

### Task 2: Perf harness, docs and release 2026.10.0

**Files:** `tests/PicoActor.EventStore.Jsonl.Tests/Performance/FileNameFormatPerf.cs`; `docs/release-notes/v2026.10.0.md`; `README.md` ×10; `picohex-actor` skill; rev-5 delta in `docs/superpowers/specs/2026-11-16-jsonl-event-store-design.md` (§Intent, §File layout, §Failure policy, §Public surface counters, §Testing — the §Testing item lists G1a, G2, G2b, G3, G4, G5a–e, G6, G7, G8, G9).

- [ ] **Step 1: opt-in harness** `[Test, Explicit]` (not run in CI): fixed GUIDs; **two independent directories** (one IdOnly, one TypePrefix, each ~4,600 files) — never both formats in one directory (an append to an id present in another format throws); one warm-up + median of 5 for `ListAggregateIdsAsync`; plus 2,000 first appends in a fresh TypePrefix directory for the non-amortizable pass. Run it, record the numbers. Commit `test(store): add the opt-in filename-format perf harness`.

- [ ] **Step 2: docs** — `README.md` + 9 translations (naming modes + migration rule); skill (option + duplicate/whole-directory rules); `docs/release-notes/v2026.10.0.md` (x bump, cost model, measured numbers + reproduction command); rev-5 delta (the five sections above, including "the rename is no longer required" in §Intent and "under `TypePrefix` a bare `<guid>.jsonl` is `OtherFormat`"). Commit `docs: document FileNameFormat and v2026.10.0`.

- [ ] **Step 3: verify**:

```bash
dotnet build PicoActor.slnx --no-restore -c Release
dotnet test --project tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release --no-build --no-progress            # 214
dotnet test --project tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release --no-build --no-progress    # 51
dotnet test --project tests/PicoActor.EventStore.InMemory.Tests/PicoActor.EventStore.InMemory.Tests.csproj -c Release --no-build --no-progress # 19
dotnet test --project tests/PicoActor.EventStore.Jsonl.Tests/PicoActor.EventStore.Jsonl.Tests.csproj -c Release --no-build --no-progress
dotnet test --project tests/PicoActor.EventStore.Gen.Tests/PicoActor.EventStore.Gen.Tests.csproj -c Release --no-build --no-progress # 3
dotnet csharpier check .
```

- [ ] **Step 4: release** (clean tree required): `./scripts/release.ps1 -DryRun` must report **API changed → v2026.10.0**; then `./scripts/release.ps1 -Push` (refreshes `api/`, commits, tags, pushes); watch Release + CI to completion; verify the Jsonl nupkg README and nuspec. Do not claim published before both are observed.

## Reverse-break (manual; run at the end of 1d, revert each)

| Mutation | Test that must go red |
|---|---|
| prefix slice → `LastIndexOf('-')` | `G2_…` (id set empty) |
| `TypePrefix` scan → per-file peek | `G2_…` (`FilesOpened != 0`) |
| remove the `EndsWith` guard | `G5c_…` (`x.txt` throws) |
| remove the whole-directory throw | `G6_…`, `G8_…` |
| `FileInfo{Exists, Length>0}` → `File.Exists` | `G5b_…` |

## Self-Review

1. **Spec coverage:** surface/parser → 1a; TypePrefix resolution/snapshots → 1b; five buckets/guard/loud throw → 1c; lazy pass/empty batch → 1d; perf/docs/release → 2. Every G (G1a, G2, G2b, G3, G4, G5a–d, G6, G7, G8, G9) has an owning task.
2. **Step scan:** each step is one action; every test has a name and an assertion (recipes for the mixed-directory tests included); commit steps are explicit with paths.
3. **Type consistency:** `FileNameFormat`, `StreamNameKind`, `Classify`, `PathFor`, `PathForNew`, `IdOnlyPathFor`, `FilesOpened`, `DirectoryScans`, `ForeignFormatFiles` — used identically across tasks.
4. **Review Focus:** five items mapped to named tests in the owning task; the perf numbers are documented as opt-in.
5. **Proportion:** plan ≈ 230 lines vs spec ≈ 250; no implementation bodies beyond the classifier contract.
