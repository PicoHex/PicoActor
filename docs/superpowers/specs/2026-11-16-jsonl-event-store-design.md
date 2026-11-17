# Generic JSONL event store — design (rev 5)

Status: revised after four spec reviews (2026-11-16). Rev 5 closes the round-4 items (C1 registration form, C2 DI ownership, C3 CPM, I1 gen embedding, I2 dependency assets, I3 samples, I4 verify-package, I5/I6 append semantics, I7 layering deltas, I8 test support, M1–M5) and fixes the round-5 table damage; Option A remains the family decision.
**Option A** (no `EventStore.Abs`; contracts stay in `PicoActor.Abs`), which removes the
`PicoActor.Abs ↔ EventStore.Abs` reference cycle, and closes the rest of round 3:

| Round-3 item | Closed by |
|---|---|
| C1 cycle | §Package and project structure: stores reference `PicoActor.Abs`; no Abs package in the family |
| C2 `AddPicoActor()` default | `### DI (C2)`: the resolve-time throw; the InMemory package ships `AddPicoActorInMemoryStore()` |
| C3 `ISnapshotableActor` | nothing moves out of `PicoActor.Abs`; the type split is unnecessary under Option A |
| C4 contract text conflict | §Failure policy + §Documentation: `GetVersionAsync` doc relaxed to a line count; `PeekFirstAsync` keeps its "null when no deserializable event" contract (skip, not throw) |
| I1–I15 | §Serialization contract (exception types), §Analyzer, §Testing (polymorphic base), §Semantics «Append» (order/empty batch/flush), §File layout (endianness), §Repo surface (CI/CPM/samples/verify-package), §Public surface (namespaces/internal counters) |
| M1–M10 | title/status, goals wording, escaping, thread-safety, counters, pack order, CPM float band, README count |

## Intent

`PicoActor` ships only `InMemoryEventStore`; the only durable store is PicoAgent's
`JsonlEventStore` (2,157 lines + a 406-line snapshot partial), a production store for that
product (ledger scans, archive/retention, checkpoints, fact scans, agent domain types). It cannot
be reused by another PicoHex consumer.

This design adds a **generic** JSONL store: one event per line, one file per aggregate, snapshots
as sidecars, single-process, AOT/source-generated serialization via PicoJetson. It implements the
contracts this repository already defines — `IEventStore`, `IEventLogReader`, `IEventCursor`,
`IEventStoreSnapshot`, `IEventStoreEnumerator` (all in `PicoActor.Abs`).

**Goal (narrow).** Behaviourally replaceable for **append / load / peek / enumerate / snapshot**.
PicoAgent adoption is an **out-of-repo follow-up**: it needs a one-way file rename
(`{type}-{id}.jsonl` → `<id:D>.jsonl`), a decision for lines only its decoder handles (framework
saga events use a second codec; unknown `$type` lines are skipped), a snapshot rewrite (no `PASJ`
header), replacements for ledger/checkpoint/fact scans and archive, and explicit acceptance of
`FlushToDisk = false` (it fsyncs every append).

## Goals

- Durable, append-only JSONL streams, one file per aggregate.
- AOT-safe, reflection-free serialization (PicoJetson source generation over the consumer's base).
- The v2026.8.0 cursor contract: a snapshot rebuild does not deserialize the head.
- `PicoActor` core no longer carries a concrete store (see §DI); the store family is three
  packages.

## Non-goals (MVP)

- Multi-process writers (concurrent writers fail loudly).
- Archive/retention, compaction, ledger/checkpoint/fact scans, secondary indexes.
- Foreign (non-event) lines in a stream.
- Encryption, compression, remote storage.
- `IAsyncDisposable` on the store (it holds no long-lived handle).
- Configurable JSON options (depth/converters): the PicoJetson defaults apply; deeply nested
  events beyond the generator's depth are unsupported.

## Package and project structure (Option A)

Contracts stay in `PicoActor.Abs`. The family adds three projects; the dependency arrows below
are acyclic by construction:

```
PicoActor.Abs (existing: IEventStore, IEventLogReader, IEventCursor,
  ↑     ↑      IEventStoreSnapshot + ActorSnapshot, IEventStoreEnumerator,
  │     │      ConcurrencyException, IDomainEvent, EventSourcedActor, ISnapshotableActor)
  │     │
  │     └── PicoActor.EventStore.InMemory   (InMemoryEventStore + the DI default extension)
  │
  └── PicoActor.EventStore.Jsonl            (JSONL store; refs PicoJetson + PicoLog.Abs)
        └── src/PicoActor.EventStore.Gen    (analyzer, IsPackable=false, embedded in Jsonl's package)
```

| Project | Contents |
|---|---|
| `src/PicoActor.EventStore.InMemory/` | `InMemoryEventStore` (moved out of `PicoActor`) + `AddPicoActorInMemoryStore()`; namespace `PicoActor.EventStore.InMemory`; references `PicoActor.Abs` only |
| `src/PicoActor.EventStore.Jsonl/` | `JsonlEventStore<TEventBase>`; namespace `PicoActor.EventStore.Jsonl`; references `PicoActor.Abs`, `PicoJetson` (CPM `2026.13.*`, **`PrivateAssets="none"`** — the default value `contentfiles;analyzers;build` writes `exclude="Build,Analyzers"` and cuts the generator plus `build/PicoJetson.targets` off every consumer), `PicoLog.Abs` (**`PrivateAssets="Build"`**, the repo convention) |
| `src/PicoActor.EventStore.Gen/` | `netstandard2.0`, `IsPackable=false`, `IsRoslynComponent=true`; embedded into the **Jsonl** package via the **two-item pair**: `ProjectReference ... OutputItemType="Analyzer" ReferenceOutputAssembly="false"` **and** `<None Include="../PicoActor.EventStore.Gen/bin/$(Configuration)/netstandard2.0/PicoActor.EventStore.Gen.dll" Pack="true" PackagePath="analyzers/dotnet/cs" />` (release.yml packs independently; a `None` without the `ProjectReference` packs a missing dll) |
| `PicoActor` (core) | keeps `ActorSystem`, options, DI; **no** concrete store |
| `tests/PicoActor.TestSupport/` (new) | `EventLogReaderConformanceBase` + a `public abstract record ConformanceEvent : IDomainEvent` with `[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]` |
| `tests/PicoActor.EventStore.InMemory.Tests/`, `tests/PicoActor.EventStore.Jsonl.Tests/` | split the store tests; no `EventStore.Abs.Tests` (nothing to assert in a contracts-only package) |
| `PicoActor.slnx`, `ci.yml`, `release.yml`, `packages.lock.json` (each new project), `api/PicoActor.EventStore.*.public.txt`, `samples/`, `scripts/verify-package.sh` | see §Repo surface |

**Namespace/GlobalUsings changes**: `src/PicoActor/GlobalUsings.cs` keeps `using PicoActor.Abs;`
(stores are referenced only where constructed); `src/**`, `tests/**`, `samples/**` that construct
`InMemoryEventStore` add `global using PicoActor.EventStore.InMemory;` (or a local using).

### DI (C2)

`PicoActor` no longer references a concrete store:

- **Throw point is resolve time**, inside the `IActorSystem` factory: if `IEventStore` cannot be
  resolved, throw `InvalidOperationException` naming `AddPicoActorInMemoryStore()` and the
  package. Registration order is therefore free (a host may call `AddPicoActor()` first), and an
  explicit `AddPicoActor(IEventStore)` still wins.
- The core `AddPicoActor(container, ActorConfig?)` keeps the config switch but **no longer
  provides an implementation**: `"InMemory"` now means "an `IEventStore` must already be
  registered"; an unknown type name still throws `NotSupportedException`, with the message
  `Supported: whatever IEventStore is already registered (e.g. AddPicoActorInMemoryStore()); other stores are constructed explicitly.`
- `PicoActor.EventStore.InMemory` provides **only** `AddPicoActorInMemoryStore()` (no second
  `AddPicoActor` overload — two same-signature extensions in one namespace would be ambiguous).
- Tests that change: `ActorConfigTests` (4 cases), `PicoActorDiExtensionsStoreTests.AddPicoActor_NoArgs_RegistersDefaultInMemoryStore` (1), `PicoActorDiExtensionsTests.AddPicoActor_WithPublisher_WiresEventOutflow`, `CommandSenderTests.AddPicoActor_RegistersSender_ResolvableFromChildScope`, and the default-store cases in `MediatorIntegrationTests` (~10 call sites).


- `PicoActor.csproj`'s `Description` and the `PicoActorDiExtensions` XML docs drop the
  "provides InMemoryEventStore" claim.

## Public surface

```csharp
namespace PicoActor.EventStore.Jsonl;

public sealed class JsonlEventStore<TEventBase> :
    IEventStore, IEventLogReader, IEventStoreSnapshot, IEventStoreEnumerator
    where TEventBase : class, IDomainEvent
{
    public JsonlEventStore(string rootDirectory, JsonlEventStoreOptions? options = null);
    // throws ArgumentException for a null/empty root, ArgumentOutOfRangeException for
    // MaxAggregateLocks <= 0; the root's trailing separator is normalized before deriving
    // <root>-snapshots
}

public sealed record JsonlEventStoreOptions
{
    public bool FlushToDisk { get; init; }        // default false
    public int MaxAggregateLocks { get; init; } = 1024;   // must be > 0
    public PicoLog.Abs.ILogger? Logger { get; init; }      // null = silent
}
```

- Explicit `IEventLogReader.PeekFirstAsync(Guid, CancellationToken)`; the plain
  `PeekFirstAsync(Guid)` (the `InMemoryEventStore` pattern). `LoadAsync` returns a **fully
  materialized `List`** (the `IEventStore.LoadAsync` contract permits a lazy view; this store
  chooses eager).
- Diagnostics: `internal long DeserializedEventCount`, `internal long TornTailLines`,
  `internal long SkippedFileNames`, `internal int LockEntryCount` (read via
  `InternalsVisibleTo("PicoActor.EventStore.Jsonl.Tests")`).
- **Logger**: `Options.Logger` is independent of the DI `ILoggerFactory`; a host that wants the
  store's warnings in its log passes the same logger explicitly (the DI extension does not wire
  it).
- **Thread safety**: one store instance per directory; two instances in one process do not share
  gates — concurrent writes to the same stream fail with an `IOException` (file sharing), which is
  the documented outcome, not retried.

## Serialization contract (critical)

PicoJetson's generator ignores **type parameters**: a `Serialize<TEventBase>(…)` call inside this
package generates nothing. Registration must happen **in the consumer's assembly**, and the
trigger is `[PicoDerivedType]` / `[PicoSerializable]`; `[PicoPolymorphic]` only names the
discriminator property.

- **One registration form only**: `TEventBase` carries `[PicoDerivedType(typeof(X), "tag")]` for
  every concrete event. `[PicoPolymorphic]` is needed only to rename the discriminator property;
  `[PicoSerializable]` on a **derived** type does **not** make `TEventBase` writable (it registers
  only the derived type; `Serialize<TEventBase>` then throws the no-serializer exception and writes
  no `$type`). PicoJetson's own `PolyContractTests` proves a base with only `[PicoDerivedType]`
  round-trips.
- **Failure modes (exact)**: an unregistered **derived** type throws `System.FormatException`
  ("Unregistered derived type for polymorphic base …; add [PicoDerivedType] …") on the first
  write; an unregistered **base** throws `InvalidOperationException("No serializer/deserializer
  registered for …")`; an unknown discriminator on read throws `FormatException("Unknown type
  discriminator: …")`. The store catches all three and rethrows a message naming `TEventBase` and
  the registration form.
- A constructor-time check is not possible: PicoJetson has no exact registration predicate
  (`HasStreamingDelegate<T>` is a proxy that misses property-less types; a probe deserialize has
  side effects).
- Dependency asset rules, separately: **`PicoJetson` → `PrivateAssets="none"`** (spelled out, not
  omitted: the DEFAULT value is `contentfiles;analyzers;build`, which writes
  `exclude="Build,Analyzers"` on the nuspec edge and cuts the generator and
  `build/PicoJetson.targets` off the consumer — "registration in the consumer assembly" then
  silently generates nothing while everything still compiles); **`PicoLog.Abs` →
  `PrivateAssets="Build"`** (the repo's convention; without it PicoLog's generator/props ride along
  to every JSONL consumer). `scripts/verify-package.sh` asserts the resulting `include="All"` edge
  on the packed package.

### Analyzer (`PicoActor.EventStore.Gen`)

Embeds in the Jsonl package (a project that only uses InMemory never pays):

- Predicate: an `IDomainEvent` implementation whose type chain is **not** listed by any
  `[PicoDerivedType]` on a base in the same compilation/references. `[PicoSerializable]` is not an
  escape hatch: a derived type with only `[PicoSerializable]` is reported too (that is the
  compile-clean, first-append-fatal shape).
- Diagnostic id fixed (`PICOSTORE001`), default severity `Info`, promotable by the project.
- **Not statically detectable** (still covered by the runtime wrap): a type registered under one
  base but stored through another, duplicate discriminators, and late `[ModuleInitializer]`
  registrations from other assemblies.

## File layout

```
<root>/<aggregateId:D>.jsonl        one JSON object per line, '\n' terminated, UTF-8 (no BOM)
<root>-snapshots/<aggregateId:D>.snap   "PASJ" + LE u64 version + LE u32 payloadLength + payload
```

- `<id:D>` is lowercase, invariant `Guid.ToString("D")`; enumeration reads the id back from the
  name. Directories are created on the first write, never by the constructor.
- Snapshots live in a sibling directory, never enumerated as streams.
- Multi-byte fields are **little-endian** via `BinaryPrimitives` (never `BitConverter`/
  `NativeEndian`); the snapshot write always uses temp + `Flush(flushToDisk: true)` + rename
  (a snapshot is an accelerator; its durability is independent of `FlushToDisk`).
- `PASJ` is the file container; the actor's `PAS1` schema envelope lives **inside** the opaque
  payload.

## Semantics (fixed)

- **Version model**: 1-based, one per event, `version ≡ count`.
- **Line content**: exactly one JSON object per line. Blank lines (after stripping ASCII spaces,
  `\t` and `\r`) are ignored and never counted; the writer emits only `\n`. The same pure
  predicate is used by the count scan, the cursor positioning and the loader.
- **Torn tail**: the **unterminated final line** (last byte is not `'\n'`) — the only shape a
  **process crash** leaves — is ignored, never counted, never thrown; reported through
  `Options.Logger` when set and counted in `TornTailLines`.
- **Corruption**: a **terminated** line that fails to deserialize is corruption and throws — the
  final line included. Power loss with `FlushToDisk = false` can leave terminated garbage; that is
  corruption: the store does **not** repair it (an operator removes the line externally). This is
  the documented cost of `FlushToDisk = false`.
- **Count / `End` / `GetVersionAsync`**: one byte scan counts complete, non-blank, `'\n'`-terminated
  lines, skipping an unterminated final line; no JSON validation; 0 for a missing stream.
  `GetVersionAsync` and `End` come from that same scan. **Documented relaxation** (four edits ship with this release): `IEventLogReader.GetVersionAsync`
  XML; layering spec `§Semantics` item 1 ("same classification rule"); layering spec `§Semantics`
  item 2 (`GetVersionAsync` error semantics — restated as "the count is a line count; a terminated
  corrupt line makes `LoadAsync`/the cursor throw while the count includes it"); layering spec
  `§Prerequisite` (P1 is **not** required for a store that throws on corruption; it is only a
  PicoAgent skip-semantics prerequisite).
- **`OpenAsync(id, fromVersion)`**: 1-based; `0` throws `ArgumentOutOfRangeException`; past the end
  is a legal empty cursor. Positions by byte offset after `fromVersion − 1` **counted** events
  (complete, non-blank, `'\n'`-terminated) — newline scan only.
  - **Never more**: `cap = fromVersion > End ? 0 : End − fromVersion + 1` (clamped before the
    subtraction); the stop condition is that yield counter, not EOF.
  - **`End`** is the count observed by this open and never changes; a stream truncated during the
    cursor's life yields fewer (MAY), never more (MUST NOT).
- **Append** (order is the contract):
  1. `events.Count == 0`: run the **read-only** witness check (no create, no repair), then the
     `expectedVersion` check, then no-op (the `InMemoryEventStore` behaviour) — an empty batch is
     not an exemption from the external-modification check.
  2. Every element must be a `TEventBase` (`ArgumentException` naming the offender; never a raw
     cast).
  3. Open `FileMode.OpenOrCreate` + `FileAccess.ReadWrite` + `FileShare.Read`.
  4. Length witness: if the file length differs from the cached witness, re-seed the cache and
     throw `ConcurrencyException` (unconditional — a write after external modification is
     refused; only the cache is re-seeded).
  5. `expectedVersion` must equal the cached count; otherwise `ConcurrencyException` — the stream
     **content** is unchanged, but a **missing** stream may be left as a zero-length file
     (`OpenOrCreate` ran; `InMemoryEventStore` has the same shape and `ListAggregateIdsAsync`
     skips empty files). Tests assert content/count, never `File.Exists`.
  6. Torn-tail repair: if `Length > 0` and the last byte is not `'\n'`, truncate to the last
     `'\n'` (or 0). Terminating the fragment is forbidden.
  7. **One** `WriteAsync` over the encoded bytes (`json` + `'\n'` per event, never split), then
     `FlushAsync(CancellationToken.None)` (the `IEventStore.AppendAsync` contract has no token);
     `Flush(flushToDisk: true)` only when `FlushToDisk`.
  8. Update the cached count and length witness; a failed write invalidates the entry.
- **Handles**: every read path (`LoadAsync`, `PeekFirstAsync`, the count scan, enumeration,
  cursors) opens `FileAccess.Read` + `FileShare.ReadWrite | FileShare.Delete`; only the writer
  takes `ReadWrite` + `FileShare.Read`. A reader that did not allow writes would make every append
  fail with a sharing violation while it lives (PicoAgent's `OpenRead` incident); there is no retry
  budget.
- **Failure policy per member**:

  | Member | Policy |
  |---|---|
  | `LoadAsync` / cursor | torn tail ignored; terminated bad line throws |
  | `PeekFirstAsync` | first line only: empty file → null; torn first line → null; a terminated line that cannot deserialize → **null** (matches `IEventStore.PeekFirstAsync`'s existing contract: "returns null when … yields no deserializable event") |
  | `ListAggregateIdsAsync` | non-`*.jsonl`/non-Guid names skipped (warn + `SkippedFileNames`); empty file skipped; a corrupt first line yields no id (via `PeekFirstAsync` → null) — enumeration never aborts boot |

- **Locks and caches**: one bounded LRU entry per aggregate holds the gate, the cached count and
  the length witness (`MaxAggregateLocks`, evict only when no holder/waiter; same rule as
  `RebuildGateTable`). Eviction costs one re-scan on the next append. **Deviation, documented**:
  PicoAgent raises `EventStoreDivergenceException` on external length changes; this store refuses
  the write with `ConcurrencyException` and re-seeds.
- **Snapshots**: `SaveSnapshotAsync` consumes/copies the payload before its `ValueTask` completes
  (the producer may reuse the buffer) and writes atomically (temp + flush + rename; a leftover
  `.tmp` is removed best-effort). `LoadSnapshotAsync` returns null (never throws) for a missing
  file, bad magic, truncated header or `version == 0`; a `payloadLength` mismatch is treated as
  absent. The read side returns a fresh array each call (the contract permits handing out the
  stored array; this implementation copies so callers need no ownership rules). The store never validates a payload or
  the snapshot version against the stream (a version above the count is returned unchanged; the
  runtime's rollback guard discards it).

## Rebuild cost

`OpenAsync(v + 1)` scans the file's bytes (page cache) to locate the offset and then deserializes
`N − v` events. The saving is **deserialization/allocation, not disk I/O** — the head is still
read once as bytes. One pass records both `End` and the offset. P1 is not needed: a generic stream
contains only events, so an un-deserializable line is corruption and throws.

## Testing

- **Conformance** (from `PicoActor.TestSupport`): `EventLogReaderConformanceBase` over a
  `JsonlEventStore<ConformanceEvent>` — `ConformanceEvent` is the new `public abstract record`
  with `[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]`; `SSnapshotAdded` inherits it (and
  moves to TestSupport as `public`; the old `internal` copy is removed because a local type of the same name would be a
  different type identity; the one
  `FindAggregateIds(nameof(SSnapshotAdded))` call site (`RebuildReaderPathTests.cs:280`) keeps
  working because the name is unchanged). `PicoActor.TestSupport` is a plain library
  (`IsTestProject=false`, no `OutputType=Exe`) that references TUnit and holds the `[Test]`
  base; both test projects reference it and their `[InheritsTests]` subclasses are discovered
  there. `PicoActor.Tests` must also add **ProjectReferences** to `PicoActor.EventStore.InMemory`
  (its ~60 files construct `InMemoryEventStore`) and to `PicoActor.TestSupport` (its
  `[InheritsTests]` subclass and the three files that still use `SSnapshotAdded`).
  Empty stream; `fromVersion` variants; past-end; `Current` pre/post; idempotent dispose;
  cancellation; a **blank-line fixture** (space/tab/`\r` variants are neither counted nor
  positioned over).
- **Serialization**: polymorphic round-trip with a base carrying **only** `[PicoDerivedType]`
  (no `[PicoSerializable]`, no `[PicoPolymorphic]`); an **unregistered derived** type fails on the
  first append with the wrapped `FormatException`; an unregistered base fails with the wrapped
  `InvalidOperationException`.
- **Append/version**: round-trip; `ConcurrencyException` on a stale `expectedVersion` (the file is
  unchanged); empty batch with a stale version still throws; empty batch with a good version
  creates nothing; a non-`TEventBase` element throws `ArgumentException`.
- **Torn tail**: a half line → load ignores it (count unchanged), the next append truncates and
  succeeds; a terminated bad line (middle **and** final) throws; power-loss corruption is not
  auto-repaired.
- **Cursor tail-only**: first `CurrentVersion == v + 1`; `DeserializedEventCount` **delta**
  equals `N − v` (read before opening, assert after the cursor completes); `OpenAsync(v+1)` on an
  unchanged stream opens once.
- **Snapshots**: save/replace/load; version > count returned unchanged; bad magic/truncated →
  null; atomic replace; little-endian header.
- **Enumeration**: matching first-event streams; non-Guid/foreign files skipped; corrupt first
  line → no id, no throw.
- **Lock bound**: `LockEntryCount` stays ≤ `MaxAggregateLocks` after touching many ids.
- **AOT proof**: extend the existing sample (published per RID and **run** by CI); `dotnet test`
  is JIT-run and proves nothing about ILC.

## Repo surface

| Change | Why |
|---|---|
| `PicoActor.slnx` | the three new src projects + two new test projects (+ TestSupport) |
| `ci.yml` | a `dotnet test --project` step per new test project; the AOT sample step keeps its 5-RID publish+run |
| `release.yml` | explicit pack order: `PicoActor.Abs` → `PicoActor.EventStore.InMemory` → `PicoActor.EventStore.Jsonl` → `PicoActor`, each with `-p:RestoreAdditionalProjectSources=…/artifacts/nupkg` |
| `packages.lock.json` | every new project (locked restore in CI) |
| `api/PicoActor.EventStore.*.public.txt` | new packages → release.ps1 reports API changed; **do not** pre-refresh |
| `samples/` | (a) add **both** `PicoActor.EventStore.InMemory` and `PicoActor.EventStore.Jsonl` project references; (b) add a `[PicoDerivedType]` event base (the existing sample events are bare `record : IDomainEvent` and cannot be used with the store); (c) append → rebuild to `Version == N` → open a **new** store instance and read `N` again → assert no sentinel file remains, in a temp dir that is deleted; `=== Done ===` stays the last line |
| `scripts/verify-package.sh` | **four edits**: (1) the `pack` section adds `src/PicoActor.EventStore.InMemory`; (2) the nuspec print loop adds that package; (3) the clean loop gains `picoactor.eventstore.inmemory`; (4) the probe references **only** that package (not `PicoActor`), plus a negative probe: `PicoActor` alone + `AddPicoActor()` must throw `InvalidOperationException` — that is the publish-face proof of C2 |
| `PicoActor.csproj` / `PicoActorDiExtensions` docs | drop "provides InMemoryEventStore"; the no-arg DI now throws without a registered store |
| `Directory.Packages.props` | add `PicoJetson` `2026.13.*`; add `PicoActor.EventStore.InMemory` and `PicoActor.EventStore.Jsonl` at `$(Version)` (the repo's own-package convention); Gen is not in CPM (`IsPackable=false`) |
| `Directory.Build.props` | nothing new (lock files + AOT/trimmable are automatic for packable projects) |

## Documentation and versioning

- `README.md` + 9 translations (**10 README files**): a "JSONL event store (opt-in)" section —
  package, base-type registration, torn-tail/corruption policy, cursor/tail-only rebuild,
  `FlushToDisk`; and the InMemory package's DI extension.
- `picohex-actor` skill: the new packages in the trigger list + durable-store subsection.
- `docs/release-notes/v<version>.md`: one **x** release for the family move + the new package
  (downstream migrates once: package refs + usings + the DI store registration), `api/` refreshed
  by `release.ps1`, downstream float band migration.

## Resolved decisions

1. `TEventBase` must be a **class**: `[PicoPolymorphic]` is `AttributeUsage(Class | Struct)`, and
   `IDomainEvent` is an interface.
2. `FlushToDisk` defaults to `false`; power-loss corruption is documented, not repaired.
3. Flat `<id>.jsonl`; sharding is a later change behind the same API.
4. No `IAsyncDisposable` on the store; cursors are disposable.
5. **No `PicoActor.EventStore.Abs`**: contracts stay in `PicoActor.Abs` (Option A). A true store
   contracts package would need `IDomainEvent`/`IEvent` to move into a shared kernel package first
   — rejected for this design.
