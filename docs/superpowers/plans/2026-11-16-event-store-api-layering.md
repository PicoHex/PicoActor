# Event Store API Layering Implementation Plan (rev 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the store read contract into a cursor-based read port and make the runtime prefer it, so a rebuild never materializes the head of an event stream.

**Architecture:** Add `IEventLogReader`/`IEventCursor` (forward-only, `End` = length at open) as an additive read port; `InMemoryEventStore` implements it; `ActorSystem.GetAsync` rebuilds through the cursor when the store is a reader (single observation for the rollback guard), and falls back to today's `LoadAsync` + `EventTailView` otherwise. `IEventStore` is unchanged.

**Tech Stack:** C# / net10.0, TUnit, PicoActor's existing store/snapshot contracts.

**Spec:** `docs/superpowers/specs/2026-11-16-event-store-api-layering-design.md` (rev 4)

## Global Constraints

- Target `net10.0`. Code-side touched files: `src/PicoActor.Abs/IEventLogReader.cs` (new), `src/PicoActor.Abs/IEventCursor.cs` (new), `src/PicoActor/InMemoryEventStore.cs`, `src/PicoActor/ActorSystem.cs`, new tests. **`IEventStore` members are not modified**; doc-only XML additions are allowed.
- Never modify `ActorSnapshot`, `IEventStoreSnapshot`, the snapshot envelope, or the single-flight gate semantics (gate held through initialization; registry re-check; failure cleanup).
- Business surface unchanged: no `IActorSystem` member is added or changed. Capability discovery is one rule — `eventStore is IEventLogReader`; **no** new `ActorSystemOptions` slot. `PicoActorDiExtensions` needs **no** change (it registers the store as-is, no wrapper).
- The rebuild observes the stream **once** per open: guard, tail and `Version` come from one cursor's `End`. Empty streams never call `rebuildFactory` — also after the full-replay re-open.
- `OpenAsync(id, 0)` throws `ArgumentOutOfRangeException`; `fromVersion > End` is legal and yields an empty cursor.
- Snapshot restore failure in the cursor path **reuses the already-constructed actor** (full replay on the same instance); it never builds a second one.
- Build 0 warnings; `dotnet csharpier format` touched files before each commit; `PicoActor.Tests` = **229** (205 + 24 new: 10 conformance + 6 reader-path + 2 pin + 4 fix-round + 2 extra conformance), `PicoActor.Abs.Tests` = 51.
- Do **not** pre-refresh `api/*.public.txt`; `scripts/release.ps1` refreshes it at release time.

## Review Focus

1. A stream that rolls back below the snapshot version (`v > End`) → full replay, **no exception** — Task 2, `RollbackGuard_ReplaysFromOne_NoThrow`.
2. A stream that shrinks while the cursor is open (`v + tail.Count != End`) → full replay, no exception — Task 2, `ShrinkingStream_DegradesToFullReplay`.
3. A stream that vanishes between the first open and the full-replay re-open → `GetAsync` returns `null`, `rebuildFactory` called 0 times — Task 2, `VanishingStream_ReturnsNull_WithoutConstructing`.
4. A store whose `LoadAsync` throws but whose reader works → rebuild succeeds through the cursor — Task 2, `ReaderOnlyStore_RebuildsWithoutLoadAsync`.
5. A store without the reader → rebuild still works through `LoadAsync` — Task 3, `NonReaderStore_FallsBackToLoadAsync`.

Out of repo (not executed by this plan): P1 `CanResolveDiscriminator`, `JsonlEventStore`, `JsonRpcStoreBinding` (stays on the fallback), and the `picohex-actor` skill text.

## File Structure

- `src/PicoActor.Abs/IEventLogReader.cs` — the read port (one type per file, matching `IEventStore.cs`).
- `src/PicoActor.Abs/IEventCursor.cs` — the cursor.
- `src/PicoActor/InMemoryEventStore.cs` — implements `IEventLogReader` (snapshot under the per-actor gate).
- `src/PicoActor/ActorSystem.cs` — `GetAsync` cursor path, `FinishRebuildAsync`, `PeekFirstAsync` helper; the `LoadAsync` fallback stays.
- `tests/PicoActor.Tests/EventLogReaderConformanceTests.cs` — conformance base + in-memory subclass.
- `tests/PicoActor.Tests/RebuildReaderPathTests.cs` — probe stores for the runtime preference and guards.
- `README*.md` (10), `docs/release-notes/v2026.8.0.md` — Task 4.

---

### Task 1: Read-port interfaces + the in-memory reader

**Files:** Create `src/PicoActor.Abs/IEventLogReader.cs`, `src/PicoActor.Abs/IEventCursor.cs`; modify `src/PicoActor/InMemoryEventStore.cs`; test `tests/PicoActor.Tests/EventLogReaderConformanceTests.cs`.

**Interfaces:**
- Produces: `IEventLogReader.GetVersionAsync/OpenAsync/PeekFirstAsync`; `IEventCursor.End/Current/CurrentVersion/MoveNextAsync/DisposeAsync`. `GetVersionAsync` has no in-repo consumer yet: it is the tools/diagnostics surface; the rebuild path uses only the `End` of `OpenAsync` (single observation).

- [ ] **Step 1: Write the failing conformance tests** (abstract base `EventLogReaderConformanceBase` with `protected abstract (IEventStore Store, IEventLogReader Reader) Create();`, seeded with three events; `InMemoryEventStore` subclass):

```csharp
[Test, Timeout(15000)] async Task GetVersion_IsZero_ForMissingStream(CancellationToken ct = default)
[Test, Timeout(15000)] async Task GetVersion_MatchesLoadAsyncCount(CancellationToken ct = default)
[Test, Timeout(15000)] async Task OpenAtOne_OnEmptyStream_EndsImmediately(CancellationToken ct = default)  // End == 0, first MoveNext false
[Test, Timeout(15000)] async Task OpenAtOne_YieldsAll_WithAscendingVersions(CancellationToken ct = default)
[Test, Timeout(15000)] async Task OpenAtV_YieldsVToEnd_WithEndUnchanged(CancellationToken ct = default)
[Test, Timeout(15000)] async Task OpenAtEndPlusOne_YieldsNothing_ButEndIsRealLength(CancellationToken ct = default)
[Test, Timeout(15000)] async Task OpenAtZero_Throws_ArgumentOutOfRange(CancellationToken ct = default)
[Test, Timeout(15000)] async Task Current_Throws_BeforeFirstMove_And_AfterEnd(CancellationToken ct = default)
[Test, Timeout(15000)] async Task DisposeAsync_IsIdempotent(CancellationToken ct = default)
[Test, Timeout(15000)] async Task MoveNext_HonorsCancellation(CancellationToken ct = default)  // pre-cancelled token -> OperationCanceledException
```

- [ ] **Step 2: Run to verify failure**

Run: `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/EventLogReaderConformanceTests/*"`
Expected: compile errors (`IEventLogReader`/`IEventCursor` missing; `InMemoryEventStore` is not a reader).

- [ ] **Step 3: Implement the interfaces and the in-memory reader**

```csharp
// src/PicoActor.Abs/IEventLogReader.cs
namespace PicoActor.Abs;

public interface IEventLogReader
{
    ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default);
    ValueTask<IEventCursor> OpenAsync(Guid actorId, ulong fromVersion, CancellationToken ct = default);
    ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId, CancellationToken ct = default);
}

// src/PicoActor.Abs/IEventCursor.cs
namespace PicoActor.Abs;

public interface IEventCursor : IAsyncDisposable
{
    ulong End { get; }                 // length at open; never changes; the cursor MUST NOT yield more
    IDomainEvent Current { get; }      // valid only after a successful MoveNextAsync
    ulong CurrentVersion { get; }
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
}
```

`InMemoryEventStore`:
- `GetVersionAsync`: under the per-actor gate, return the stream count (0 when absent); same classification as `LoadAsync` (it is the same list).
- `OpenAsync`: `fromVersion == 0` → `ArgumentOutOfRangeException`; under the gate copy the stream to an array (`End = array.Length`) and return a cursor over it starting at index `fromVersion - 1` (clamped to `End` → empty). The array copy is the snapshot that makes the cursor safe against concurrent `AppendAsync` — the same invariant `LoadAsync`'s `ToList()` under the gate provides.
- `PeekFirstAsync`: unchanged.
- `ct.ThrowIfCancellationRequested()` at the start of `OpenAsync`/`MoveNextAsync`; `DisposeAsync` idempotent and does not affect an already-read `Current`.

- [ ] **Step 4: Run to verify pass**

Run: `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/EventLogReaderConformanceTests/*"`
Expected: 10/10 PASS.

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor.Abs/IEventLogReader.cs src/PicoActor.Abs/IEventCursor.cs src/PicoActor/InMemoryEventStore.cs tests/PicoActor.Tests/EventLogReaderConformanceTests.cs
git add src/PicoActor.Abs/IEventLogReader.cs src/PicoActor.Abs/IEventCursor.cs src/PicoActor/InMemoryEventStore.cs tests/PicoActor.Tests/EventLogReaderConformanceTests.cs
git commit -m "feat(abs): add the cursor-based event log reader and the in-memory implementation"
```

---

### Task 2: Rebuild through the cursor (preference, guard, PeekFirst)

**Files:** Modify `src/PicoActor/ActorSystem.cs`; test `tests/PicoActor.Tests/RebuildReaderPathTests.cs`.

**Interfaces:**
- Consumes: Task 1's reader/cursor; existing `ISnapshotableActor`, `IEventSourcedActor`, `CompleteInitializationAsync`, `DiscardDuplicateAsync`, `ResolveAfterInitialization`, the `Func<object>` rebuild factory from `_rebuildFactories`.
- Produces: `private async ValueTask<T?> RebuildFromReaderAsync<T>(IEventLogReader reader, Guid id, Func<object> rebuildFactory) where T : IActor`; `private async ValueTask<T?> FullReplayAsync<T>(IEventLogReader reader, Guid id, Func<object> rebuildFactory) where T : IActor`; `private async ValueTask<T?> FinishRebuildAsync<T>(ActorBase actor, Guid id) where T : IActor`; `private static async ValueTask<List<IDomainEvent>> DrainAsync(IEventCursor cursor)`; `private async ValueTask<List<IDomainEvent>> DrainAllAsync(IEventLogReader reader, Guid id)`; `private ValueTask<IDomainEvent?> PeekFirstAsync(Guid id)`.

- [ ] **Step 1: Write the failing tests**

Probe contracts (`tests/PicoActor.Tests/RebuildReaderPathTests.cs`):
- `ReaderOnlyStore` : `IEventStore`, `IEventStoreEnumerator`, `IEventLogReader`, `IEventStoreSnapshot`, backed by an `InMemoryEventStore`.
  - `IEventStore.LoadAsync` and `IEventStore.PeekFirstAsync` throw `NotSupportedException("reader-only probe")`.
  - `AppendAsync`, `ListAggregateIdsAsync`, `LoadSnapshotAsync`, `SaveSnapshotAsync` and `IEventLogReader.PeekFirstAsync` all delegate to the inner store — seeding and snapshot setup must work.
  - `OpenAsync` delegates to the inner store but applies two knobs **to the first `OpenAsync` call only** (later opens see the real stream): `EndOverride` (report a smaller `End`) and `YieldLimit` (yield at most N events).
- `VanishingStore`: same, but **both** opens yield 0 while reporting `End = 6`.

Seeding recipe: `NewSystem(store)` + `Register` + `CreateAsync<SSnapshotCounter>(new SSnapshotCreate(0))` + `AskAsync` adds (as `RebuildTailViewTests.BuildAsync`), then `StopAsync`; snapshot payload = magic `0x5A` + `BitConverter.GetBytes(value)` (`RebuildTailViewTests.cs:112-117`).

Tests (all `[Test, Timeout(15000)]` + `CancellationToken cancellationToken = default` + `_ = cancellationToken;`):

```csharp
ReaderOnlyStore_RebuildsWithoutLoadAsync        // 4 events, no snapshot; rebuilt != null, Value == 4, MutateCalls == 4
ReaderOnlyStore_PeekFirst_UsesReader            // FindAggregateIds("SSnapshotAdded", _ => true) returns the id; IEventStore.PeekFirstAsync throws
RollbackGuard_ReplaysFromOne_NoThrow            // 3 events + stale snapshot v=5 (value 4) -> full replay: MutateCalls == 3, Value == 3, no throw
ShrinkingStream_DegradesToFullReplay            // 6 events + snapshot v=5; first open yields 0 -> second open yields all: MutateCalls == 6, Value == 6
VanishingStream_ReturnsNull_WithoutConstructing // snapshot v=5, End=6, both opens yield 0 -> null, factoryCalls == 0
SnapshotRebuild_DrainsOnlyTheTail_ThroughTheCursor // 100 events + snapshot v=90; cursor records versions -> 10 yields, min == 91
```

- [ ] **Step 2: Run to verify failure**

Run: `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/RebuildReaderPathTests/*"`
Expected: 6/6 FAIL — `LoadAsync` throws on every probe, so the old path cannot pass any of them (no accidental greens).

- [ ] **Step 3: Implement the cursor rebuild**

In `GetAsync`, inside the gate after the registry re-check:

```csharp
if (_eventStore is IEventLogReader reader)
    return await RebuildFromReaderAsync<T>(reader, id, rebuildFactory).ConfigureAwait(false);
// else: today's path, calling the same FinishRebuildAsync<T> at its tail
```

```csharp
private async ValueTask<T?> RebuildFromReaderAsync<T>(
    IEventLogReader reader, Guid id, Func<object> rebuildFactory) where T : IActor
{
    var snapshot = _eventStore is IEventStoreSnapshot ss
        ? await ss.LoadSnapshotAsync(id).ConfigureAwait(false) : null;
    var v = snapshot?.Version ?? 0;

    await using var cursor = await reader.OpenAsync(id, v == 0 ? 1UL : v + 1UL).ConfigureAwait(false);
    if (cursor.End == 0) return default;                                     // no factory

    var tail = await DrainAsync(cursor).ConfigureAwait(false);
    if (v > cursor.End || v + (ulong)tail.Count != cursor.End)               // rollback / shrink
    {
        await cursor.DisposeAsync();                                        // release before the second open
        return await FullReplayAsync<T>(reader, id, rebuildFactory).ConfigureAwait(false);
    }

    var actor = (ActorBase)(IActor)rebuildFactory();
    WireActor(actor, id);
    var es = (IEventSourcedActor)actor;

    Exception? failure = null;
    if (v == 0)
    {
        es.ReplayEvents(tail);
    }
    else if (es is ISnapshotableActor snap && TryApplySnapshot(snap, snapshot, cursor.End, out failure))
    {
        es.ReplayEventsFrom(v, tail);
        TryLog(l => l.Info($"Actor {typeof(T).Name} {id} restored from snapshot v{v}, replayed {tail.Count} of {cursor.End} events"));
    }
    else
    {
        // Snapshot rejected (schema/format/throw) or the actor cannot restore one: replay the
        // WHOLE stream on THIS instance — never build a second actor.
        await cursor.DisposeAsync();                                        // release before the second open
        var all = await DrainAllAsync(reader, id).ConfigureAwait(false);
        if (all.Count == 0) { await DiscardDuplicateAsync(actor); return default; }
        es.ReplayEvents(all);
        TryLog(l => l.Warning($"Actor {typeof(T).Name} {id}: snapshot ignored, replaying the full stream ({failure?.Message ?? "actor cannot restore a snapshot"})"));
    }

    return await FinishRebuildAsync<T>(actor, id).ConfigureAwait(false);
}
```

- `FullReplayAsync<T>(reader, id, rebuildFactory)`: `await using var c = await reader.OpenAsync(id, 1)`; drain; `all.Count == 0 → return default` (no factory); build + wire; `ReplayEvents(all)`; `FinishRebuildAsync`.
- `DrainAsync(IEventCursor) → List<IDomainEvent>`: `while (await cursor.MoveNextAsync()) list.Add(cursor.Current)`.
- `DrainAllAsync(reader, id)`: `await using var c = await reader.OpenAsync(id, 1); return await DrainAsync(c);`.
- `FinishRebuildAsync<T>(ActorBase actor, Guid id)`: the existing tail of `GetAsync` verbatim — terminal-saga check (stop + return default), `_registry.TryAdd` with the duplicate-discard branch, `CompleteInitializationAsync`, the generic `rebuilt from events: {id} (v{es.Version})` log, `NoteReactivated`, `ResolveAfterInitialization`. **It does not touch the gate**; the gate stays owned by `GetAsync`'s `finally`.
- `TryApplySnapshot(ISnapshotableActor snap, ActorSnapshot? snapshot, ulong streamEnd, out Exception? failure)`: `snapshot is null || snapshot.Version == 0 || snapshot.Version > streamEnd` → false; `RestoreSnapshot` throws → false with the exception; otherwise true. Extract it from `RestoreOrReplayAsync` so both paths share it — the reader path passes `cursor.End`, the fallback passes `(ulong)events.Count`.
- `PeekFirstAsync(Guid id)`: `_eventStore is IEventLogReader r ? r.PeekFirstAsync(id) : _eventStore.PeekFirstAsync(id)`; call it from `FindAggregateIds` (`ActorSystem.cs:863`) and `ResumeInterruptedSagasAsync` (`:891`).

- [ ] **Step 4: Run to verify pass**

Run: `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/RebuildReaderPathTests/*"`
Expected: 6/6 PASS, and the whole `PicoActor.Tests` suite stays green.

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor/ActorSystem.cs tests/PicoActor.Tests/RebuildReaderPathTests.cs
git add src/PicoActor/ActorSystem.cs tests/PicoActor.Tests/RebuildReaderPathTests.cs
git commit -m "perf(actor-system): rebuild through the event cursor with a single-observation guard"
```

---

### Task 3: Fallback and reader-path single-flight pins

**Files:** Test `tests/PicoActor.Tests/RebuildReaderPathTests.cs` (add two tests); no production change expected.

**Interfaces:** none new (`CountingStore` here is a plain `IEventStore` wrapper, no reader).

- [ ] **Step 1: Write the tests**

```csharp
NonReaderStore_FallsBackToLoadAsync      // CountingStore: LoadAsync calls == 1, rebuilt != null, Value == 4
ConcurrentMisses_SingleFlight_OnReaderPath_Unchanged // two concurrent GetAsync on the reader path: same instance, factoryCalls == 1
```

> The second pin is **not** redundant with `RebuildLoadOnceTests.ConcurrentMisses_RebuildOnceAndReadOnce`: that store wraps `IEventStore` only, so it exercises the fallback. This one pins single-flight on the **reader** path.

- [ ] **Step 2: Run** — `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/RebuildReaderPathTests/*"` → expected 8/8 green from Task 2; if either fails, Task 2 regressed the fallback/single-flight and must be fixed, not the test.

- [ ] **Step 3: Commit**

```bash
dotnet csharpier format tests/PicoActor.Tests/RebuildReaderPathTests.cs
git add tests/PicoActor.Tests/RebuildReaderPathTests.cs
git commit -m "test(actor-system): pin the LoadAsync fallback and reader-path single-flight"
```

---

### Task 4: Verification, API delta and documentation

**Files:** `README*.md` (10), `docs/release-notes/v2026.8.0.md`, doc-only XML in `src/PicoActor.Abs/IEventStore.cs`.

- [ ] **Step 1: Full verification**

```bash
dotnet build PicoActor.slnx --no-restore -c Release
dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release --no-build --no-progress
dotnet test tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release --no-build --no-progress
dotnet csharpier check .
```
Expected: 0 warnings; `PicoActor.Tests` = 229; `Abs.Tests` = 51; CSharpier exit 0.

- [ ] **Step 2: Confirm both API deltas without touching the baselines**

```bash
dotnet run --project scripts/api-surface -- dump src/PicoActor.Abs/bin/Release/net10.0/PicoActor.Abs.dll --out artifacts/api-abs.txt
dotnet run --project scripts/api-surface -- diff api/PicoActor.Abs.public.txt artifacts/api-abs.txt
dotnet run --project scripts/api-surface -- dump src/PicoActor/bin/Release/net10.0/PicoActor.dll --out artifacts/api-picoactor.txt
dotnet run --project scripts/api-surface -- diff api/PicoActor.public.txt artifacts/api-picoactor.txt
```
Expected: `PicoActor.Abs` gains `IEventLogReader`/`IEventCursor` (types + members); `PicoActor` gains `InMemoryEventStore`'s new interface row (`I:`) and its two `M:` rows; the existing `AppendAsync/LoadAsync/PeekFirstAsync` lines are unchanged. `diff` exits 1 when surfaces differ — expected.

- [ ] **Step 3: Documentation** — `IEventStore.LoadAsync` XML: add "tools should read through `IEventLogReader`; this member stays as the runtime fallback" (doc-only). README ×10: business code uses `IActorSystem`; `IEventStore` is a store-author SPI; `IEventLogReader` is the tool/projection surface; reader stores deserialize only the tail. Release note `v2026.8.0`: x bump (two new interfaces + `InMemoryEventStore` implementation), downstream float band must migrate, P1 is a cross-repo prerequisite, `JsonRpcStoreBinding` stays on the fallback. Commit:

```bash
git add README*.md docs/release-notes/v2026.8.0.md src/PicoActor.Abs/IEventStore.cs
git commit -m "docs: document the event-log read port and the v2026.8.0 API delta"
```

- [ ] **Step 4: Dry run, then release** (clean tree is required by `release.ps1`):

```bash
./scripts/release.ps1 -DryRun     # must report: API changed -> v2026.8.0
./scripts/release.ps1 -Push       # refreshes api/, commits, tags, pushes
```
Then watch the Release workflow and CI to completion; unpack the published nupkg and confirm its `README.md` contains the read-port text.

## Self-Review

1. **Spec coverage:** interfaces + in-memory reader → Task 1; runtime preference/guard/empty/PeekFirst → Task 2; fallback/single-flight → Task 3; docs/versioning → Task 4. P1, `JsonlEventStore`, `JsonRpcStoreBinding` and the skill are listed as out-of-repo.
2. **Step scan:** every step is one action; Task 1 has 10 named tests, Task 2 has 6 with assertions + probe contracts, Task 3 has 2; implementation steps carry signatures/algorithms only.
3. **Type consistency:** `IEventLogReader`, `IEventCursor` (`End`, `CurrentVersion`), `RebuildFromReaderAsync<T>`, `FullReplayAsync<T>`, `FinishRebuildAsync<T>`, `TryApplySnapshot`, `PeekFirstAsync` — same names across tasks.
4. **Review Focus:** five lines, each mapped to a named test in the owning task.
5. **Proportion:** plan = 306 lines, spec ≈ 285; no implementation body beyond the algorithm skeleton.
