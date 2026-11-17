# Event store API layering — design (rev 4)

Status: revised after three spec reviews (2026-11-16). Rev 4 closes N6–N8 from the third
review: the empty check also applies after the full-replay re-open (N6), `End` is specified as
"exactly when the stream is unchanged, best-effort fewer when it shrinks" (N7), and the
serializer probe is no longer disguised as a non-goal (N8).

## Problem

One public interface, `IEventStore`, carries three audiences and one prescriptive return type:

| Member | Who uses it | What it forces |
|---|---|---|
| `AppendAsync(id, expectedVersion, events)` | the actor write path (runtime) | — |
| `LoadAsync(id) → IReadOnlyList<IDomainEvent>` | the rebuild path **and** tools/projections | every store must produce a `Count`-known, index-addressable, fully deserialized list |
| `PeekFirstAsync(id) → IDomainEvent?` | `FindAggregateIds` (`ActorSystem.cs:863`) and `ResumeInterruptedSagasAsync` (`:891`) | — |

Tools really do call `LoadAsync` (not the enumerators): `SessionStreamFactSource.ReadOneAsync`
(`ArchiveSweepSvc.cs:122`), `BranchOutcome.cs:86`, `KernelBootSvc.cs:883`,
`MirrorReconciler.cs:195/210`, `JsonlEventStore.cs:2121`, `PicoAgent.Tools/Packages/*`. The
enumerators give ids or ledger/fact projections, not event lists.

Consequences (PicoAgent `JsonlEventStore.LoadAsync`):

- a snapshot-accelerated rebuild that only needs the events after the snapshot still
  deserializes the whole file (82,367 events / 12 MB chat streams, 13,413 events / 25.4 MB
  session streams);
- the `IReadOnlyList` return type is an implementation decision inside a persistence contract.
  The v2026.7.1 zero-copy `EventTailView` buys **nothing** for the production store —
  `LoadAsync` is still an eager `List.Add` loop — the measured proof that only a cursor can
  move the cost;
- business readers see an "events" method on the interface they know as "the store".

## Goals

- **No business-facing member returns an event collection.** `IActorSystem` returns actors,
  ids (`FindAggregateIds`), or results (`ResumeInterruptedSagasAsync`). The two recovery
  entries still take an event type/predicate; documenting them as recovery-path APIs and
  moving them out of `IActorSystem` is a separate design.
- The runtime's persistence port says **what it needs**: append events; read the events from a
  version onward, in order, observing the stream length **once**.
- Tools/projections get an explicit **event-log reader** instead of borrowing the write port.
- Existing stores compile and keep working (additive first).
- A file store can avoid constructing domain events for the head: the head is **classified**
  (the `$type` resolved against the same registry the loader uses), the tail is deserialized.

## Non-goals

- A store that returns actors. Actor construction needs the registered rebuild factory, wiring,
  snapshot decoding and saga semantics; those belong to the runtime.
- A new serialization format or payload (the snapshot envelope stays opaque). The
  discriminator probe P1 is a **cross-repo prerequisite** (PicoSerDe / PicoAgent.Domain), see
  §Prerequisite — it is not excluded by this non-goal.
- Streaming the write side (appends are batched and small).
- Changing snapshot retention/trigger semantics.
- Removing `LoadAsync`/`PeekFirstAsync` from `IEventStore` in this change.

## Prerequisite (serializer layer)

The file store's cheap head classification needs a **discriminator probe** from the
serialization layer: "would this `$type` deserialize under the current registry?".
Today the loader's skip rule is "try `DomainEventSerializer.Deserialize`, treat a
`FormatException` mentioning the discriminator/`$type` as a skippable line"
(`JsonlEventStore.cs:901-941`), and the registry lives inside generated `Deserialize` code
(`PicoSerDe/…PicoJetson.g.cs:94`). There is no way to ask that question without a full
`Deserialize`.

- **Prerequisite P1**: a public probe on the serializer layer, e.g.
  `bool CanResolveDiscriminator(string typeName)` over **the same** registry used by
  `Deserialize`. It is a PicoSerDe / PicoAgent.Domain change, not part of `PicoActor`.
  **P1 is not a prerequisite of the generic JSONL store** (v2026.9.0): a stream that holds only
  events has no skippable lines, so an un-deserializable line is corruption and throws — the
  head costs a byte scan, not a deserialization, without any discriminator probe. P1 is only the
  prerequisite for PicoAgent's **skip** semantics (foreign/unknown-`$type` lines), which
  `PicoActor.EventStore.Jsonl` deliberately does not have.
- Until P1 exists, a file store must either (a) parse each head line into a `JsonDocument`
  just to read `$type` and compare it against a **copy** of the registry — rejected, because a
  copy drifts and drift corrupts the tail offset — or (b) deserialize the head exactly as
  `LoadAsync` does, in which case the cursor's benefit is reduced to "the head is not retained
  in the replay list". This design requires P1 for the file store's full benefit and states
  that reduction explicitly.

## Options

### A. Documentation only — rejected

No version bump, but does not fix the prescription and gives tools no read contract.

### B. Split the list contract — rejected

Breaking for every implementer (15+ test stores, `JsonlEventStore`, `JsonRpcStoreBinding`),
contradicting "existing stores keep compiling", and keeps full deserialization.

### C. Split + cursor (recommended)

```csharp
// Runtime write port.
public interface IEventStore
{
    ValueTask<ulong> AppendAsync(Guid actorId, ulong expectedVersion, IReadOnlyList<IDomainEvent> events);
}

// Read port.
public interface IEventLogReader
{
    /// Current event count (0 = no stream). For tools and diagnostics; the rebuild path does
    /// NOT call this — it takes the length from the cursor's End (one observation).
    ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default);

    /// Forward-only cursor starting at fromVersion (1-based, inclusive).
    ValueTask<IEventCursor> OpenAsync(Guid actorId, ulong fromVersion, CancellationToken ct = default);

    /// The first deserializable event (version 1), or null.
    ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId, CancellationToken ct = default);
}

public interface IEventCursor : IAsyncDisposable
{
    /// Stream length (event count) observed by this OpenAsync — the SAME observation the
    /// cursor will yield from. The rollback guard and the tail check both use this value.
    ulong End { get; }

    IDomainEvent Current { get; }      // valid only after a successful MoveNextAsync
    ulong CurrentVersion { get; }      // 1-based, strictly ascending
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
}
```

`End` is the fix for the two-observation hole: today the guard (`snapshot.Version >
events.Count`), the tail slice and the final `Version` all come from one `LoadAsync`
(`ActorSystem.cs:285/416/441`); with `End` they come from one `OpenAsync` again.

### D. Store returns the actor — rejected

See Non-goals.

## Recommended shape (C, additive first)

1. **Add** `IEventCursor` and `IEventLogReader` (public, `PicoActor.Abs`). `End` and
   `CurrentVersion` are required members.
2. **Keep** `IEventStore` unchanged (three members, unchanged semantics).
3. **Runtime preference**: when the event store implements `IEventLogReader`, rebuild through
   `OpenAsync`; otherwise fall back to `LoadAsync` + `EventTailView`.
4. **`InMemoryEventStore` implements `IEventLogReader`** in this step: `OpenAsync` takes a
   snapshot under the per-actor gate (copy or count + indexed reads with bounds checks), so a
   concurrent `AppendAsync` cannot tear the cursor — the same invariant `ToList()` under the
   gate provides. `End` is that snapshot's count.
5. **Tools** use `IEventLogReader` for tail/partial reads. Whole-stream projections
   (`SessionStreamFactSource`, `BranchOutcome`, `MirrorReconciler`, `KernelBootSvc` tombstone
   scans, `TaskSessionOutcomeProjection`, `SessionRetentionPolicy`) keep `LoadAsync`: they need
   the whole stream, and the list is their requirement.
6. **Capability discovery**: one rule — `eventStore is IEventLogReader`. No new
   `ActorSystemOptions` slot.

### Completion criterion (decidable)

(a) `InMemoryEventStore` implements `IEventLogReader`.
(b) a store whose `LoadAsync` throws but whose `IEventLogReader` works still rebuilds through
the cursor in a loading test.
(c) `IEventStore` has the same three members with the same semantics.

Removing `LoadAsync`/`PeekFirstAsync` is a later x release and requires every in-use store to
implement the reader first (including `JsonRpcStoreBinding`, or its retirement).

## Semantics (fixed)

- **Versions**: 1-based, one per event, `version ≡ count`. `GetVersionAsync` returns 0 for a
  missing/empty stream. The count and `OpenAsync`'s positioning use the **same** cheap scan of
  the underlying medium — complete, non-blank, terminated lines (records) — never a
  deserialization: a blank line is neither counted nor positioned over, and an unterminated
  final line (the crash tail) is neither counted nor thrown.
- **`GetVersionAsync` error semantics** (revised with v2026.9.0: the count is a LINE count, not a
  validity check): for a **clean** stream it equals `(await LoadAsync(id)).Count`. For a damaged
  one it may exceed that number: a TERMINATED line that fails to deserialize makes `LoadAsync`
  and the cursor throw, while the count scan still counts it (counting must not require a
  deserializer). The last-line tolerance is the same shape as the loader's, but not the same
  exception surface: a torn final line is skipped by both; a corrupt terminated line throws in
  `LoadAsync` and the cursor. A store may therefore report a version for a stream it cannot
  load — the count is a length, the loader is the validity gate.
- **`OpenAsync(id, fromVersion)`**: `fromVersion` is 1-based; `0` throws
  `ArgumentOutOfRangeException`. A value beyond the end is **allowed** and yields an empty
  cursor whose `End` is the actual length — the rollback path depends on this, and it is what
  replaces the old "throw on out-of-range" rule.
- **`End`**: the event count observed by this open; it never changes for the cursor's lifetime.
  When the underlying stream is NOT modified while the cursor is open, the cursor yields
  exactly the events with version in `[fromVersion, End]`. If the stream is truncated, rolled
  back or archived during the cursor's life, the cursor **MAY yield fewer** (best effort, reads
  up to what remains) but **MUST NOT yield more**. The runtime's `v + tail.Count != End` check
  is what turns the "fewer" case into a full replay — it is not a store-contract violation.
- **Ordering**: strictly ascending `CurrentVersion`, no duplicates, no skipped versions.
- **After the end**: `MoveNextAsync` keeps returning `false`; `Current`/`CurrentVersion` throw
  `InvalidOperationException`.
- **Before the first success / after disposal**: `Current`/`CurrentVersion` throw
  `InvalidOperationException`.
- **Mid-stream corruption**: `MoveNextAsync` throws the same exception `LoadAsync` would for
  that line; it never stops silently on a bad line.
- **Dispose**: releases the handle/lock, idempotent, does not affect an already-read `Current`.
- **Cancellation**: observed at `OpenAsync`/`MoveNextAsync` I/O waits; an already-produced
  `Current` is unaffected.
- **Concurrency**: a cursor is a read snapshot taken at `OpenAsync` (in-memory under its gate;
  file at its length then). Appends after open may or may not be visible; the rebuild gate
  serializes rebuilds, and a racing external append is handled by the optimistic-concurrency
  conflict path. Archiving during a read yields events up to the archive point, no exception.
- **Rollback guard (kept, single observation)**: with a snapshot at `v`, open at `v + 1` and
  use the cursor's `End`:
  - `v > End` → discard the snapshot, replay from version 1 (`OpenAsync(id, 1)`);
  - drain the tail; if `v + tail.Count != End` → discard and replay from version 1 (a stream
    that shrank between open and drain is the rollback case, and it must degrade, not throw);
  - otherwise `ReplayEventsFrom(v, tail)`; `Version` equals `End`.
  No normal rollback scenario throws: `OpenAsync` beyond the end is legal, and a short drain is
  a mismatch, not an exception.

## Rebuild algorithm (cursor path)

```
cursor = OpenAsync(id, v + 1)            // v = 0 when there is no usable snapshot
if cursor.End == 0: dispose; return default       // NO rebuildFactory call (unchanged)

tail = drain(cursor)                     // List<IDomainEvent>, tail only
if v > cursor.End or v + tail.Count != cursor.End:
    dispose
    all = drain(OpenAsync(id, 1))        // the full replay is a SECOND open
    if all.Count == 0: return default    // NO rebuildFactory call (the stream vanished)
    ReplayEvents(all)
    return
ReplayEventsFrom(v, tail)
```

- The runtime **drains the cursor into a `List<IDomainEvent>`** before replaying: the actor
  contract (`ReplayEventsFrom(ulong, IReadOnlyList<IDomainEvent>)`,
  `ReplayEvents(IReadOnlyList<IDomainEvent>)`, `Version = baseVersion + events.Count`) is
  unchanged. The goal is "no **full-stream** materialization": the head is not turned into
  domain events, the tail is materialized. `ISnapshotableActor` gains no cursor overload.
- The empty check happens **before** `rebuildFactory()` on **every** open: the first one
  (`End == 0`) and the full-replay re-open (`all.Count == 0`). A stream that vanishes between
  the two opens therefore returns `default` instead of constructing a Version-0 actor.
- `OpenAsync`/`MoveNextAsync` exceptions propagate exactly as `LoadAsync` exceptions do today
  (`GetAsync` does not swallow them).

## Migration

| Consumer | Today | After |
|---|---|---|
| `ActorSystem.GetAsync` rebuild | `LoadAsync` + `Skip` | reader: `OpenAsync(v+1)` + `End` guard; fallback `LoadAsync` + `EventTailView` |
| `FindAggregateIds` (`:863`) | `PeekFirstAsync` | reader `PeekFirstAsync`; fallback `IEventStore.PeekFirstAsync` |
| `ResumeInterruptedSagasAsync` (`:891`) | `PeekFirstAsync` | reader `PeekFirstAsync`; same fallback |
| In-memory store | list store | implements `IEventLogReader` (snapshot-under-gate cursor) |
| `JsonlEventStore` (PicoAgent) | eager `LoadAsync` | implements `IEventLogReader`: classify the head via prerequisite P1, deserialize only the tail |
| `JsonRpcStoreBinding` (PicoAgent) | `IEventStore` only | **not** a reader in this step (a cursor needs a new RPC verb); stays on the fallback |
| Tools: tail/partial reads | `LoadAsync` | `IEventLogReader` |
| Tools: whole-stream projections | `LoadAsync` | **keep** `LoadAsync` (list semantics are their requirement) |

Versioning: adding the two interfaces is an **x bump** (additive public API). `IEventStore` is
unchanged, so no implementer breaks.

## Testing

- **Cursor conformance** — one reusable abstract base, inherited by `InMemoryEventStore` and a
  probe store (the `exactly [V, End]` assertions below hold only while the stream is unchanged;
  shrinking/archival cases use their own probe store): `GetVersionAsync` 0 for a missing stream; `OpenAsync(1)` on an empty stream →
  first `MoveNext` false, `End == 0`; `fromVersion = 1` yields all; `fromVersion = V` yields
  `V..End`; `fromVersion = End + 1` → empty cursor, `End` correct; `Current` throws before the
  first success and after the end; `DisposeAsync` idempotent; cancellation observed;
  `OpenAsync(0)` throws `ArgumentOutOfRangeException`.
- **Runtime prefers the cursor**: a store whose `LoadAsync` throws but whose reader works —
  rebuild succeeds.
- **Tail-only cost**: a probe cursor that counts constructed `IDomainEvent` instances — a
  rebuild with a snapshot at `V` constructs exactly `End - V` events and classifies the head
  **once** (a counter for classification passes == 1).
- **Rollback guard (no throw)**: a reader reporting `End` smaller than the snapshot version →
  snapshot discarded, full replay from 1 (`Mutate` count == actual events), no exception.
- **Shrinking stream**: `End` reports `N` at open, the drain yields `N - 1 - v` → snapshot
  discarded, full replay, no exception.
- **Vanishing stream**: `End` reports `N`, the tail drain yields 0, the full-replay re-open also
  yields 0 → `GetAsync` returns `null` and `rebuildFactory` is called **0** times.
- **Fallback**: a store without the reader still rebuilds through `LoadAsync`
  (`RebuildLoadOnceTests`/`RebuildTailViewTests` stay green).

## Documentation surface

- `README` (all languages) and the `picohex-actor` skill: business code uses `IActorSystem`;
  `IEventStore` is a store-author SPI; `IEventLogReader` is the tool/projection read surface.
- **Do not pre-refresh** `api/*.public.txt`; `scripts/release.ps1` refreshes it at release
  time. The release note names the two new interfaces and says this is an x release (downstream
  must migrate its float band).

## Resolved decisions

1. `PeekFirstAsync` stays on `IEventStore` as the fallback until every in-use store implements
   the reader; no deprecation timeline in this design.
2. `CurrentVersion` and `End` are required cursor members: they are the ordering contract and
   the single-observation anchor of the rollback guard.
3. No `SkipToAsync(version)`: `OpenAsync(fromVersion)` is the only positioning primitive. No
   consumer needs a ranged skip, and a member that can silently skip events only makes missing
   events a legal option.
