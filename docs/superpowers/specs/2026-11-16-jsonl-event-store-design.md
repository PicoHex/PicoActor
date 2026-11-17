# Generic JSONL event store — design (rev 5 + stream-naming delta + event-type-binding delta)

**Rev-6 delta — event type bindings (`EventTypeBindings`).** Added after the rev-5 / naming round and
marked inline where it applies: §Public surface (`EventTypeBindings` + the option property),
§Failure policy «Bindings and framework events», §Testing (`Bnd0`–`Bnd21`). The delta makes a **type
outside `TEventBase`** writable and readable — the framework's own `SagaCompleted`/`SagaFailed` first,
because `SagaActor` persists them into the aggregate's stream in the same flush — and it is **additive
on the wire**: a stream of `TEventBase` events is byte-identical to rev 5, an unrecognised `$type` still
takes the `TEventBase` path, and the table is empty until a consumer or the store puts something in it.

**Rev-5 delta — stream naming (`FileNameFormat`).** Added after the round-5 fixes and marked
inline where it applies: §Intent (the rename is gone), §File layout «Name layouts», §Failure
policy «Directory and name level», §Public surface (`FileNameFormat` + option), §Testing (G1a–G9).
The delta is **name-level only**: the line format, the `PASJ` container and the cursor contract
are unchanged, and `IdOnly` (the default) reproduces rev 5 byte for byte.

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
PicoAgent adoption is an **out-of-repo follow-up**: it needs a decision for lines only its decoder
handles (framework saga events use a second codec; unknown `$type` lines are skipped), a snapshot
rewrite (no `PASJ` header), replacements for ledger/checkpoint/fact scans and archive, and explicit
acceptance of `FlushToDisk = false` (it fsyncs every append). It needs **no file rename** — see the
delta below.

**Rev-5 delta — §Intent: the "one-way file rename" no longer holds.** Rev 5 assumed the new store's
`<id:D>.jsonl` was the only layout, which turned `<type>-<id>.jsonl` (what PicoAgent already writes)
into a migration debt: every stream renamed once, offline, with no way to tell a half-migrated
directory from a healthy one. `FileNameFormat.TypePrefix` removes that debt at the **name** level:

- `<type>-<id:D>.jsonl` becomes a first-class layout (prefix = `events[0]`'s concrete type name,
  **fixed at creation**; enumeration resolves ids from names with **zero file opens**), so pointing
  this store at a PicoAgent directory is a configuration change, not a migration.
- The rename is not merely *tolerated*, it is **diagnosed**, and detection is **symmetric**: under
  `TypePrefix` a bare `<id>.jsonl` is the other layout, under `IdOnly` a prefixed name is. Pointing a
  store at a directory of the other layout is therefore a loud `InvalidOperationException` naming
  both counts — never a silently empty system, which is how a whole history disappears from view.
- `IdOnly` stays the **default** and stays the historical layout (names, enumeration, counts), so
  nothing above changes for an existing deployment; the mixed-directory machinery is what makes
  *either* direction safe, and it costs an `IdOnly` store nothing until it is actually mixed.

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
    public FileNameFormat FileNameFormat { get; init; }   // default IdOnly; an undefined value
                                                          // throws ArgumentOutOfRangeException
    public PicoLog.Abs.ILogger? Logger { get; init; }      // null = silent
}

// rev-5 delta: the NAME-level contract. The line format is identical in both modes.
public enum FileNameFormat
{
    IdOnly = 0,      // <id:D>.jsonl — default, the historical layout, reproduced byte for byte
    TypePrefix = 1,  // <type>-<id:D>.jsonl — prefix = events[0].GetType().Name, fixed at creation
}
```

```csharp
// rev-6 delta: the event types this store may write and read IN ADDITION to TEventBase.
public sealed class EventTypeBindings
{
    internal EventTypeBindings();                             // the OPTIONS owns the table (D1): no
                                                              // free-floating registration
    public EventTypeBindings Add<T>(string tag)                // ROUTING ONLY (no serializer, no codegen)
        where T : class, IDomainEvent;
    public int RegisteredCount { get; }                        // consumer bindings + applied defaults
    public int FrameworkDefaultCount { get; }                  // the defaults actually APPLIED (0..2)
}

public sealed record JsonlEventStoreOptions
{
    public EventTypeBindings EventTypeBindings { get; } = new();   // empty; the STORE seeds the defaults
}
```

- **The tag is the `$type` a bound event is written with**, spliced verbatim, and the value the read path
  dispatches on (`Ordinal`, first member only). One type ⇄ one tag: a second `Add<T>` for the same type
  with another tag, or a second type on a taken tag, throws at the call; the same pair again is an
  idempotent no-op. The grammar is non-blank and free of `"`, `\` and control characters, because the
  splice is unescaped and the damage would only surface on read-back.
- **The table is frozen and copied at construction** (`FrozenDictionary` per direction): the append and
  read paths take no lock, and `Add` after a store exists throws naming what froze the table instead of
  registering a rule that applies to nothing. A second store over the same options instance sees the same
  frozen copy (no double-seeding).
- **The store owns the framework defaults** (`TryAdd`, one seeding mechanism): `SagaCompleted →
  PicoActorSagaCompleted` and `SagaFailed → PicoActorSagaFailed`. A consumer binding on either side of
  the pair wins, so `FrameworkDefaultCount` reports what was APPLIED (`0..2`) and `RegisteredCount` the
  total — the pair a boot check asserts. Each wire tag has ONE owner: the package's
  `FrameworkEventTags` constant (the store's seeds reference it), and nothing else spells the literal.
- **The framework events' serializers are the PACKAGE's generation point**, not the consumer's:
  `FrameworkEventSerializers.Generate()` holds four closed call sites (`SerializeToUtf8Bytes` and
  `Deserialize` for each of the two records), whose generated registration is process-wide. It exists
  because PicoJetson is call-site driven and the store is generic: `[GenerateSerializer(typeof(T))]`
  cannot stand in — the attribute pipeline reads members only and never a primary constructor, so on a
  positional record it emits `new T()` + init-only assignments and the package stops compiling
  (**CS7036**/**CS8852**). A consumer's own bound type therefore needs a generation point **in its own
  assembly** (a literal call), and that is what `Add<T>` does not do for it.
- **The binding belongs to the OPTIONS, not to the store**: `JsonlEventStore<TEventBase>` reads
  `options.EventTypeBindings` once, at construction, and never again.

- Explicit `IEventLogReader.PeekFirstAsync(Guid, CancellationToken)`; the plain
  `PeekFirstAsync(Guid)` (the `InMemoryEventStore` pattern). `LoadAsync` returns a **fully
  materialized `List`** (the `IEventStore.LoadAsync` contract permits a lazy view; this store
  chooses eager).
- Diagnostics: `internal long DeserializedEventCount`, `internal long TornTailLines`,
  `internal long SkippedFileNames`, `internal long FilesOpened`, `internal long DirectoryScans`,
  `internal long ForeignFormatFiles`, `internal int LockEntryCount` (read via
  `InternalsVisibleTo("PicoActor.EventStore.Jsonl.Tests")`).
  - `FilesOpened` — read handles opened (a load, a cursor move, a count, a peek; a **write** handle is
    not an open for this counter). It is the number that states the `TypePrefix` win: one open per
    stream under `IdOnly`, **zero** under `TypePrefix`.
  - `DirectoryScans` — classification passes. Every enumeration pays one in both modes (that pass is
    where a foreign name, an unparseable id, the other layout and a whole-directory
    misconfiguration are diagnosed). `TypePrefix` pays one more at construction (the map) and one per
    resolved miss — but the two paths pay it differently, which is the whole point of the negative
    cache: a READ remembers the miss (single-flight, so a burst of probes of one absent id pays one
    pass), while an APPEND never trusts that memory and re-scans, because a remembered absence is not
    evidence that the stream is still absent. `IdOnly` pays exactly ONE more and only lazily, at the
    first observation of a stream it does not have — a store that only touches streams it already has
    never scans.
  - `ForeignFormatFiles` — entries classified as the *other* layout, **MONOTONIC** because it is the
    gate of the `IdOnly` duplicate guard: once any pass has seen another layout here, appends check.
    It counts NAMES, which is what that gate needs; the whole-directory diagnosis asks the other
    question and counts HISTORIES (`otherNonEmpty`, the foreign entries that are not ZERO-LENGTH), so
    a directory of other-layout leftovers is an empty store, not a misconfigured one.
  - `NegativeCacheInvalidations` — remembered misses dropped by a successful append of THAT id.
    **Per id, never wholesale, and outside the scan gate** (`ConcurrentDictionary.TryRemove`): an
    append that learns one prefix falsifies no other id's absence, and the hot path must not serialize
    on the gate a directory pass holds. Identically `0` under `IdOnly`, where no miss is ever
    remembered.
- **The constructor can throw `InvalidOperationException`** (rev-5 delta): under `TypePrefix` it runs
  the map pass, so a root whose streams are all in the other layout fails at construction instead of
  at the first enumeration. Under `IdOnly` construction still touches nothing (no scan, no directory,
  no throw) — the same misconfiguration surfaces at the first observation of a missing stream.
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

### Name layouts (rev-5 delta)

`JsonlEventStoreOptions.FileNameFormat` chooses how a stream **file is named**. The line format is
identical in both layouts, so the name is the only thing that distinguishes a stream of one from a
stream of the other — which is exactly why a mixture has to be diagnosed rather than merged.

| `FileNameFormat` | Stream file | Enumeration cost |
|---|---|---|
| `IdOnly` = `0` (**default**) | `<root>/<id:D>.jsonl` | one file OPEN per stream: the type is in the content, so the first event must be read |
| `TypePrefix` = `1` | `<root>/<type>-<id:D>.jsonl` | zero opens: the type and the id come from the names |

- `{type}` is `events[0].GetType().Name` — the concrete type of the stream's **first persisted
  event**, backtick arity kept, never `FullName`. An empty prefix is impossible by construction:
  only a non-empty batch creates a stream, and the batch is what supplies the type (`PathForNew`).
- **The prefix is fixed at creation.** An existing stream keeps the name it was born with, whatever a
  later batch's first event is called (G7). The `id → type` map is learned from the names (one pass
  at construction, under `TypePrefix` only) and completed by every classification pass, so a cold
  start costs one pass — and a resolved **miss** costs one more, never one per id.
- **The negative cache is the read path's.** The ids a miss failed to resolve are remembered, bounded
  at 4,096 (the set is emptied past the capacity, never grown) and shared single-flight through the
  same gate a directory pass holds: the winner scans, a reader that arrived meanwhile answers from the
  double-check. Three consequences are deliberate, not accidental:
  (1) **an append re-scans** — resolving a remembered absence to "no stream" there would fork a second
  name beside a stream an external writer created after the map was built (B9), and it never WRITES
  the cache either, since the entry it would record is the one that append falsifies (B10);
  (2) **the invalidation is per id** — B11: dropping a new id's own entry costs neither a wholesale
  clear nor the gate, so the append hot path never pays for a cache it does not read;
  (3) **a read may be STALE** by one stream: a miss (or a map hit) is a fact about the map this
  instance saw, an absent stream that appeared beside it is found by the next pass, and a new instance
  re-reads everything. That is the stated price of the 4,096-id memory.
- **Name grammar lives in one place** (`JsonlNames`): strip `.jsonl` only **after** an `EndsWith`
  guard (a `x.txt` is shorter than the suffix, and a bare slice throws inside a boot-time scan), then
  split the type prefix at the **first** `-` — a `D` GUID contains four hyphens, so a
  `LastIndexOf('-')` split would hand back the trailing twelve hex digits as the "id".
- **Snapshots are name-format independent**: `<root>-snapshots/<id:D>.snap`, keyed by the aggregate
  **id**, never by the stream's name. A snapshot survives its stream being archived, deleted or never
  written at all, and the `PASJ` container is unchanged (G4).

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

### Directory and name level (rev-5 delta)

Every scan is ONE classification pass (`JsonlNames.Classify`), and every directory entry lands in
exactly one bucket. That is what turns a mixed directory into a diagnosis instead of an empty
enumeration:

| Bucket | Meaning | Counter / report |
|---|---|---|
| configured match | configured layout, name parsed (prefix + id) | none — a candidate for the query |
| configured, other type | configured layout, prefix ≠ the queried `firstEventType` | **none**: many stream types in one directory is the NORMAL case, not a mixture |
| other format | valid only under the *other* layout's rule (symmetric) | `ForeignFormatFiles` (monotonic) + **one** Warning per scan, not one per file |
| unparseable id | configured SHAPE whose suffix is not a `D` GUID | `SkippedFileNames` + one warning per name |
| foreign entry | anything else (a README, a lease, a non-`.jsonl` name, an empty stem) | `SkippedFileNames` + one warning per name |

- **A whole directory in the other layout THROWS**: `configured == 0 && otherNonEmpty > 0` →
  `InvalidOperationException` naming the root, both counts, both formats and the two ways out
  (migrate the directory, or set `FileNameFormat`). It is raised from **every** scan — enumeration,
  construction (a `TypePrefix` map pass) and the `IdOnly` lazy pass — because "empty system" is the
  one answer that lets an operator keep writing into a history it cannot see. Both sides of the
  comparison count HISTORIES: a ZERO-LENGTH configured name does not make a directory configured, and
  a ZERO-LENGTH foreign one does not make it misconfigured (G6b, B5).
- **Two other shapes are named instead of absorbed.** A **root that exists as a FILE**
  (`RootIsAFile`) raises `IOException` — nothing can be enumerated, opened or created there — from
  the enumeration and from any scan; a stream **NAME occupied by a DIRECTORY**
  (`ThrowIfOccupiedByDirectory`) raises `InvalidOperationException` naming the obstacle, on the read
  path and the append path alike, in both formats. Neither is a bare sharing violation and neither may
  decay into "empty aggregate".
- **A failed lazy pass is LATCHED, and the only recovery is a new instance.** The misconfigured
  directory, an unreadable root and a root that is a file all publish the FAILURE (three states, not a
  bool): every later observation re-raises the same exception, `DirectoryScans` stops at one, and the
  store never quietly starts answering "empty aggregate". There is no reset member — the directory is
  fixed and the store is constructed again, which is also what re-reads a directory a read had gone
  stale on.
- **Duplicate guard** (`AppendAsync`, before the encode, so a refused append has touched no file):

  | Mode | Check | Cost |
  |---|---|---|
  | `TypePrefix` | the other layout's name IS derivable from the id | one `new FileInfo(path) is { Exists: true, Length: > 0 }` stat per append, ungated |
  | `IdOnly` | the other layout's prefix lives in the NAME and is not derivable | one directory pass per append, gated on `ForeignFormatFiles > 0` |

  A **zero-length** file is not a stream (rev 5 lets a failed append leave one), so it never blocks an
  append; only a non-empty second history does. The price is stated, not hidden: `TypePrefix` pays one
  stat per append — the `<id>.jsonl` layout's advantage is precisely that it needs none — and `IdOnly`
  pays a directory pass per append only where a scan has already reported the directory as mixed.
- **Two prefixes for one id** (`A-<id>.jsonl` **and** `B-<id>.jsonl` under `TypePrefix`) are two
  histories of one aggregate: the classification pass records the conflict, the append refuses and
  lists both candidates, and the store never invents a third name for a stream it will not extend.
- **An empty batch** validates `expectedVersion` and creates nothing in either mode: no duplicate
  guard, no file, no directory, no lazy pass (G9).
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

### Bindings and framework events (rev-6 delta)

Every refusal below happens **where the operator's fix belongs**, never as a symptom later on a read:
the table refuses at the call, the construction refuses before anything is frozen, and the append
refuses at its FIRST bound write — with no file created and no silent substitute.

| Situation | Where | Exception |
|---|---|---|
| `Add<T>` for a type already bound to another tag, or a tag already naming another type | at the `Add` call | `InvalidOperationException` naming both tags/types; the same pair again is a no-op |
| blank tag, or one containing `"`, `\` or a control character | at the `Add` call | `ArgumentOutOfRangeException` — the tag is spliced unescaped, so the damage would surface on read-back |
| `Add<T>` after a store was constructed over those options | at the `Add` call | `InvalidOperationException` naming the store that froze the table (the store COPIES it, so the rule would apply to nothing) |
| `TEventBase` already covers `SagaCompleted`/`SagaFailed` (`IDomainEvent` itself, or any base they derive from) | at **construction**, before the freeze | `InvalidOperationException`: they would be both ordinary and bound events — two routes, hence two tags — and the message says to use a class base of your own and let `EventTypeBindings` carry the framework events |
| a bound type assignable to `TEventBase` (the framework defaults make this reachable) | at **construction**, after the seed | `InvalidOperationException`: one type, one registration |
| a bound type with **no generation point** in any loaded assembly | at the **first append** that carries it | `InvalidOperationException` naming the type and the missing serializer, with the remedy (a closed call site in the consumer's assembly). Nothing is written: no partial batch, no truncated line, no fallback encode as `TEventBase` |
| a `$type` the table does not know, on read | the read path | **not a failure**: unconditional fallback to `Deserialize<TEventBase>`, so a stream with custom discriminators of its own — or one written before the tags existed — reads exactly as before |

- **Mixed batches do not degrade the single-write contract.** `TEventBase ∪ bound` in one `AppendAsync`
  is one buffer, one write and one flush: a bound element never starts a second batch, opens a second
  handle or pays a second fsync, and a batch refused for any reason above has written nothing.
- **The naming freedom is not a failure either.** Under `TypePrefix`, `unknown-<id>.jsonl` — a prefix no
  registered *type* owns, so a classification match that no type query asks for — is reachable by id,
  invisible to `ListAggregateIdsAsync`, not counted `ForeignFormatFiles`, and it never triggers the
  whole-directory `InvalidOperationException`: a host's own categorisation is not a misconfiguration.
- **No alias layer.** The two framework tags are permanent wire format. A consumer binding may REPLACE a
  default (and then writes its own tag), but the store never rewrites, translates or guesses a tag.

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

### Naming (rev-5 delta): `tests/PicoActor.EventStore.Jsonl.Tests/FileNameFormatTests.cs`

Counter assertions are **deltas** (read before, assert after), never totals, so each case states the
cost model instead of a snapshot of it.

- **G1a — `IdOnly` is untouched.** A store over a pre-written v2026.9.0 directory: `DirectoryScans`,
  `ForeignFormatFiles` and `FilesOpened` are `0` after construction; an append to the existing stream
  resolves with no scan (`DirectoryScans` still `0`); exactly one `<id:D>.jsonl` is on disk; the
  enumeration that follows pays exactly ONE pass and opens ≥ 1 file.
- **G2 — `TypePrefix` enumerates the NAMES, zero opens.** Three streams, then a COLD instance: the
  enumeration moves `FilesOpened` by `0` and returns the same three ids as the `IdOnly` control,
  whose enumeration moves it by `3`. The id set is asserted on its own (an empty set satisfies "zero
  opens" too), and the on-disk names must be `<type>-<id:D>.jsonl`.
- **G2b — the two buckets that must not be confused.** A stream of ANOTHER type under `TypePrefix` is
  neither counted nor logged; a configured-SHAPE name with an unparseable id is one
  `SkippedFileNames` plus one warning, with no exception.
- **G3 — the read path is format independent.** Same events, two roots: `GetVersionAsync`, cursor
  `Current`, peek and load answer identically. One documented deviation, asserted both ways: an id
  whose FIRST line is corrupt is still enumerated under `TypePrefix` (its name is valid, nothing is
  opened) and is not under `IdOnly` (the corrupt first line yields no id).
- **G4 — snapshots are keyed by id.** `SaveSnapshotAsync` for an id with NO stream writes
  `<root>-snapshots/<id:D>.snap`, creates no stream file, and keeps the `PASJ` bytes unchanged; a NEW
  `TypePrefix` instance reads it back with `FilesOpened == 0`.
- **G5a — one id in BOTH layouts, non-empty in the other one**: the append throws before a byte is
  written (`GetVersionAsync` unchanged) — the store never opens a second history.
  `G5a_OtherFormatNonEmpty_AppendThrows` (TypePrefix) and `G5a_IdOnly_OtherFormatNonEmpty_AppendThrows`
  (IdOnly, where the other layout's name is not derivable and the check costs the gated directory pass).
- **G5b — the other layout's file is ZERO-LENGTH**: the append succeeds; a failed append's leftover
  is not a stream. `G5b_OtherFormatZeroLength_AppendSucceeds` (TypePrefix) and
  `G5b_IdOnly_OtherFormatZeroLength_AppendSucceeds` (IdOnly).
- **G5b' / `G5f` — TWO zero-length other-layout leftovers for one id**: still zero histories, so the
  append succeeds and the multi-candidate refusal must not fire over leftovers
  (`G5f_IdOnly_TwoZeroLengthOtherFormat_AppendSucceeds`).
- **G5c — a short foreign name (`x.txt`)**: one `SkippedFileNames`, one warning, no throw — the
  `EndsWith` guard precedes the slice exactly for this.
- **G5d — ONE warning per SCAN**: two other-format entries count two in `ForeignFormatFiles` but
  produce a single report; a half-migrated directory must not become a warning storm.
- **G5e — duplicate prefixes**: under `IdOnly` (prefixed candidates, gate open after a scan) and
  under `TypePrefix` (both names in the configured format) the append refuses, names both files and
  writes no third name — both directions, because "foreign" depends only on the configured layout.
- **G6 — a `TypePrefix`-only directory under `IdOnly` options throws**
  `InvalidOperationException` from the ENUMERATION with `configured: 0` and `other-format: N` in the
  message; the mirror case (an `IdOnly`-only directory under `TypePrefix`) throws from the
  CONSTRUCTOR, because that is where its map pass runs.
- **G6b — the diagnosis counts NON-EMPTY histories (`otherNonEmpty`)**: a ZERO-LENGTH configured-format
  name does not make the directory configured, so a directory of other-layout histories plus one
  leftover still throws instead of silently enumerating an empty system
  (`G6b_ZeroLengthConfiguredName_WithOtherFormat_Throws`); the converse — a directory of nothing but
  other-layout leftovers — is an empty store and stays silent (`B5`).
- **G7 — a renamed event type is format dependent.** A stream renamed to `RetiredType-<id>.jsonl`:
  `IdOnly` still finds it (content), `TypePrefix` answers it only for `RetiredType` (name) and
  reports nothing as skipped; a later append keeps the retired prefix, and no
  `SSnapshotAdded-<id>.jsonl` appears beside it.
- **G8 — a host that never enumerates still gets the diagnosis.** Loads/appends only, against a
  directory in the other layout: the FIRST missing-stream observation (a LOAD counts, not just an
  append) throws, the failed pass is LATCHED (the next observation throws again instead of decaying
  into "empty aggregate"), `DirectoryScans` stops at one, and the refused append forks no second
  stream.
- **G9 — an empty batch validates the version and creates nothing.** Under `TypePrefix`: a stale
  `expectedVersion` throws `ConcurrencyException` with the right `ActorId`/expected/actual; a current
  one is a no-op answering the observed version; `DirectoryScans` does not move; neither `<id>.jsonl`
  nor a second name appears.

### The miss path (rev-5 delta, second pass): `B1`…`B11`

Counter assertions stay deltas. The claim the B cases state together is that the **negative cache is the
read path's**, single-flight, bounded, invalidated per id — and that an append is never stale.

- **B1 — one pass for a BURST of concurrent misses** (`B1_TypePrefix_ConcurrentMisses_ShareOnePass`):
  eight readers of an id nobody has move `DirectoryScans` by exactly `1`; the single-flight is the
  double-check inside the gate, not a lock per reader.
- **B1b — a repeated miss costs nothing** (`B1b_TypePrefix_RepeatedMiss_ScansOnce`): the second and
  third observations of the same absent id, through two different members, pay no pass.
- **B1c — an append drops that id's entry** (`B1c_TypePrefix_AppendClearsTheRememberedMiss`): the
  append pays ONE pass (it re-scans — see B9), the two reads that follow pay none (map hit), and
  `NegativeCacheInvalidations` moves by `1`. A miss recorded for ANOTHER id survives the map growth and
  keeps costing nothing: that staleness is the price of the per-id drop, and it is stated in the case.
- **B2 — a rival prefix that arrives after the map is found by the next SCAN**
  (`B2_TypePrefix_RivalPrefixAfterTheMap_IsFoundByTheNextScan`): only an external file changes the
  directory, the read and the append both answer from the map with no pass, the next miss pays the one
  pass that records the conflict, and every later append for that id is refused naming both files.
- **B2b — the ENCODING window** (`B2b_AppendEncodingWindow_RivalPrefixIsRegisteredThroughLearnPrefix`):
  a prefix learned while the batch was encoding is REGISTERED as a conflict rather than dropped by a
  silent `TryAdd`; this is the seam, not the window above.
- **B3…B8 — the surface around the miss**: the lazy pass latches an `IOException`
  (`B3_LazyPass_LatchesIoFailure`, root as a FILE), a mixed IdOnly directory's guard pass is counted
  (`B4`), leftovers of the other layout are not a misconfiguration (`B5`), `FilesOpened` counts only
  successful opens (`B6`), a stream NAME occupied by a DIRECTORY is diagnosed on both paths (`B7`), and
  a read of a conflicted id answers while warning once (`B8`).
- **B9 — an append never trusts a remembered absence**
  (`B9_TypePrefix_AppendAfterRememberedMiss_RescansAndRefuses`): read-miss → an external writer creates
  `Other-<id>.jsonl` → a read may stay stale → the append re-scans, resolves THAT stream and throws
  `ConcurrencyException(expected 0, actual 1)`; no `<type>-<id>.jsonl` is forked and the rival still
  holds its one event.
- **B10 — the empty batch obeys the same rule**
  (`B10_TypePrefix_EmptyBatchAfterRememberedMiss_Rescans`): it resolves its path like a writing append,
  pays exactly the one re-scan, reports version `0` as a `ConcurrencyException` against the real count
  and creates nothing.
- **B11 — the drop is PER ID, and the hot path takes no gate**
  (`B11_NewIdAppend_DropsOnlyItsOwnRememberedMiss`): a new id's first append leaves
  `NegativeCacheInvalidations` at `0` and another id's remembered miss still costs no pass; the append
  of the id that WAS remembered moves it to `1`. Under `IdOnly` it stays `0` for a structural reason —
  the id IS the name, so nothing is ever remembered there.

### Bindings (rev-6 delta): the suites under `tests/PicoActor.EventStore.Jsonl.Tests/`

`Bnd0`…`Bnd21` state the binding contract (the write path, the read path, the two construction refusals, the tag-clash probe and the sniffing edge cases), and **the numbers below follow the suites as shipped** — the
cases live in THREE files, not one: `EventTypeBindingTests.cs` (the table itself, plus one separate class
for the generation tripwire), `BoundEventWriteTests.cs` (the append path) and `BoundEventReadTests.cs`
(the read path). Each case is asserted **where the operator's fix belongs**: at the `Add` call, at the
construction, or at the first append that carries the bound event. Nothing here asserts a wall-clock
number; element and flush counts, the exact bytes on disk, and the log are the observable.

#### `EventTypeBindingTests.cs` — the table, the freeze, the counts

- **Bnd0 — the generation point exists** (`Bnd0_FrameworkEventSerializer_IsGenerated`, in that file's
  second class `FrameworkSerializerTripswireTests`): the framework serializers are observed **from another
  class's** closed delegate call sites through the public `JsonSerializer` facade — `SerRegistry` and
  `DesRegistry` must both be populated, `SagaCompleted(null)` encodes to `{"Result":null}` and
  `SagaFailed("boom")` to `{"Reason":"boom"}`, and both decode back. Deleting
  `FrameworkEventSerializers.Generate()` (or replacing its closed call sites with an attribute marker)
  turns this case red. It is the tripwire for the PicoSerDe limitation, not a data test.
- **Bnd6 — one tag, one type** (`Bnd6_DuplicateTag_ThrowsAtTheCall`): a second `Add` on a taken tag throws
  at the call, names the tag and both types, and registers nothing (`RegisteredCount` unchanged). The
  idempotence row is asserted the way the READER defines a tag: the same type with the same tag — in
  either case, because the generated reader matches a discriminator value case-insensitively — is a no-op.
- **Bnd6b — one type, one tag** (`Bnd6b_SameTypeDifferentTag_ThrowsAtTheCall`): the second tag for a bound
  type throws at the call and names BOTH tags; `RegisteredCount` stays at one.
- **Bnd7 — a bound type that is also `TEventBase`** (`Bnd7_BoundTypeAlsoTEventBase_ThrowsAtConstruction`):
  binding the store's own event (`SSnapshotAdded`, which derives from `ConformanceEvent`) is refused at the
  construction; the message names the bound type and the generic argument it collides with, and offers
  exactly ONE remedy — delete the `Add` line, because "register the event's own base type as the generic
  argument" is already the case and would fail again. The refusal lands before anything is frozen: the
  same options instance still accepts a binding afterwards, and the framework seeds the refused
  construction would have applied are rolled back (`RegisteredCount` = the consumer's bindings,
  `FrameworkDefaultCount` = `0`).
- **Bnd8 — the counts and the start-up line** (`Bnd8_Counts`): a fresh options instance reports `0`/`0` and
  logs nothing (the CONSTRUCTOR is the one seeding point); a store over untouched options reports `2`/`2`;
  both canonical tags are pinned by literal in both directions, a miss is a `false` and never a throw; and
  the single Info record carries the FROZEN numbers verbatim —
  `event type bindings: 2 registered (2 framework-default)`.
- **Bnd14 — a consumer override wins** (`Bnd14_ConsumerOverrideWins`): binding `SagaCompleted` to a
  consumer tag before constructing the store keeps that tag in the frozen table the store reads, the other
  default still applies (`TryAdd`, never all-or-nothing), and `FrameworkDefaultCount` reports the
  reduction.
- **Bnd16 — the tag grammar** (`Bnd16_TagGrammar_RejectsQuotesBackslashAndBlanks`): blank,
  whitespace-only, tab, quote, backslash, newline, NUL and ESC are refused with
  `ArgumentOutOfRangeException`; a null tag is the argument-shape `ArgumentNullException`; nothing was
  registered by any of the refusals; and the canonical tag is still accepted (`RegisteredCount` = `1`).
- **Bnd17 — the freeze** (`Bnd17_AddAfterFreeze_Throws`): `Add` after a store was constructed over those
  options throws naming the store, and the live store's view is the copy taken at construction. The tag
  GRAMMAR is still diagnosed first on a frozen table (an illegal tag stays an illegal-tag error), and a
  SECOND construction over the same options does not seed again.

#### `BoundEventWriteTests.cs` — the append path

- **Bnd1 — a mixed batch is one append** (`Bnd1_MixedBatch_RoundTrips`): `TEventBase` and a bound event in
  one `AppendAsync` give version `2`, two lines in order, the whole file byte-for-byte
  (`{"$type":"ss-added","Delta":7}\n{"$type":"PicoActorSagaCompleted","Result":null}\n`), `TornTailLines`
  at `0` and no Warning in the log.
- **Bnd1b — the zero-member payload** (`Bnd1b_ZeroMemberPayload_WritesTheTagAlone`): a memberless bound
  event (a consumer-owned `MilestoneReached`) writes `{"$type":"milestone"}` — the separator after the tag
  is conditional, so no dangling comma is produced.
- **Bnd2 — a saga-only stream is named after the completion** (`Bnd2_SagaOnlyStream_PrefixIsTheTypeName`):
  under `TypePrefix` the file is `SagaCompleted-<id:D>.jsonl` — never the store's base type, never the tag
  — and a COLD store enumerates it with `FilesOpened` still `0`.
- **Bnd5 — neither base nor bound** (`Bnd5_UnboundEvent_IsRefused_WithTheRemedy`): an event that is only an
  `IDomainEvent` is refused with the EXISTING wording preserved verbatim (including the offending `index`),
  the second remedy (`EventTypeBindings`) appended, and nothing created on disk.
- **Bnd10 — the naming format changes a name, never a line** (`Bnd10_IdOnly_And_TypePrefix_Agree`): the
  same mixed batch written under `IdOnly` and under `TypePrefix` (separate roots) produces byte-identical
  streams and the same version.
- **Bnd15 — `Result` is `null`, or nothing is written**
  (`Bnd15_NullResult_RoundTrips_NonNullFailsLoudly`): `SagaCompleted(null)` writes the corpus line and
  decodes back through the CONCRETE deserializer, reached as a method group so this assembly generates
  nothing and Bnd0's tripwire stays blind. A non-null `Result` is blocked UPSTREAM by PicoJetson's writer,
  and the test pins what the store owes: a loud failure with **no stream, no version**. It goes red on
  purpose the day the serializer learns untyped members.

#### `BoundEventReadTests.cs` — the read path

- **Bnd3 — the corpus tag reads, and rewrites byte-identical**
  (`Bnd3_ExistingCorpusTag_IsReadable_AndRewritesByteIdentical`): a `PicoActorSagaCompleted` line that no
  `[PicoDerivedType]` declares is read as `SagaCompleted` **only by the binding**, and what came off the
  disk goes back on with the same tag, member order and bytes.
- **Bnd4 — an unknown tag is loud and names what IS bound** (`Bnd4_UnknownTag_IsLoud`): the line falls back
  to `TEventBase` and fails with the pre-existing `FormatException`, whose message still names the tag and
  now also both framework tags this store could have read.
- **Bnd4b — the sniff agrees with the reader about what a tag is**
  (`Bnd4b_DiscriminatorIsMatchedCaseInsensitively`): `$TYPE` as a member name and a differently-cased tag
  value both resolve; a rewrite emits the REGISTERED spelling.
- **Bnd4c — a bound tag whose payload does not fit**
  (`Bnd4c_PayloadThatDoesNotFitTheTag_IsWrappedByTheBinding`): the binding's own message names the tag, the
  bound type, the file and where the tag came from (`framework default`), and never offers the
  `[PicoDerivedType]` remedy that belongs to the base route.
- **Bnd9 — routing is not serialization**
  (`Bnd9_BoundTypeWithoutGeneratedSerializer_IsRefusedOnFirstWrite`): a bound type with no generation point
  fails on its FIRST WRITE with `InvalidOperationException` naming the type, the tag and the missing
  serializer, with no file and no version.
- **Bnd11 — a non-type prefix is reachable by id, not by type**
  (`Bnd11_UnknownPrefix_IsReachableById_NotByType`): `unknown-<id>.jsonl` loads by id, is invisible to
  `ListAggregateIdsAsync(nameof(SagaCompleted))`, is **not** counted `ForeignFormatFiles`, is not a skipped
  name and raises no Warning.
- **Bnd12 — an interface base cannot carry the registration**
  (`Bnd12_InterfaceBase_IsRefusedAtConstruction`): `JsonlEventStore<IDomainEvent>` throws before anything is
  frozen or created, and the message names the KIND of type that is wrong (`interface`,
  `[PicoDerivedType]`) rather than a missing registration line.
- **Bnd13 — only the FIRST member is a discriminator** (`Bnd13_NonFirstDiscriminator_IsTreatedAsAbsent`): a
  bound tag in the second position is "no discriminator", so the line fails on the EXISTING route with the
  polymorphic reader's own `FormatException` (wrapped) and never with a binding message.

So the two refusals of D5 sit in different files — `Bnd12` (the base cannot be an interface) and `Bnd7` (a
bound type that is also the base) — and the "same pair again" row is asserted inside `Bnd6` rather than in a
case of its own. A later delta continues from `Bnd21`; the numbering above is the suite, not a plan.

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
  `FlushToDisk`, a "Stream naming" subsection (`FileNameFormat`, both modes, the PicoAgent
  zero-rename migration, mixed-directory diagnosis, default unchanged); and the InMemory package's
  DI extension.
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
6. **`FileNameFormat.IdOnly` stays the default** (rev-5 delta). `TypePrefix` is opt-in: it buys an
   enumeration with zero opens and costs one `stat` per append, and a default that renamed files
   would make every v2026.9.0 directory a migration on upgrade. The two layouts share the line
   format, so the choice is reversible for a directory that was never mixed; a mixed one is
   diagnosed in both directions rather than merged.
7. **The negative cache serves reads only, and it is invalidated per id** (B9…B11). The alternative —
   trusting a remembered absence on the append path — forks a second name for an id that already has a
   stream; the other alternative — dropping the whole cache whenever the map grows — has to take the
   scan gate, so every first append of every id would serialize the store, and on `IdOnly` it would
   maintain a cache that is never read. The price, a read that stays stale until the next pass, is
   accepted and documented: an external writer's file is a directory-level event, and the append (the
   only path that must never be stale) does pay the one pass a new id costs anyway.
8. **No directory pass is added to the append path to close the rival-prefix window** (B2). It would
   return exactly what the `id → type` map buys, on the one path that pays per append; the next
   classification pass records the conflict and every later append for that id is refused.
