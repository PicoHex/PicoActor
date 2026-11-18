# Event-type bindings — recording framework events in the JSONL store (rev 5)

Status: revised after two spec reviews and the first implementation probe (2026-11-18). Rev 4 records that the
`[GenerateSerializer]` **attribute form is broken for positional records** (PicoSerDe's attribute pipeline extracts
members but not the primary constructor → CS7036/CS8852), so the shipped mechanism is the **closed call-site form**,
and that a test-assembly invocation would mask a missing generation point (the tripwire must use method groups).
Rev 3 closed both reviews' C1–C3, I1–I10, M1–M8 (the splice
byte recipe, the tag grammar, the Add/freeze semantics matrix, the discriminator fallback, the distinct error message,
and the corpus/assertion evidence gaps). Rev 2 closed the first round. Scope:
`PicoActor.EventStore.Jsonl`. Version: **x bump (2026.11.0)** — additive public API (`EventTypeBindings`, one options
property).

## Problem

`JsonlEventStore<TEventBase>` binds the writable event set to one CLR base type, twice: write
(`JsonlEventStore.cs:1445` — `is not TEventBase` → `ArgumentException`) and read (`:1482` —
`Deserialize<TEventBase>`). A saga stream necessarily contains the framework's own terminal event
(`SagaActor.MarkComplete` appends `SagaCompleted` **in the same batch** as business events — `SagaActor.cs:78-127`),
and those records live in `PicoActor.Abs` **without deriving from the consumer's base type**: `[PicoDerivedType]` may
only name genuine subtypes, so an application base cannot adopt them (their `sealed`-ness is incidental — it prevents
further derivation, not adoption):

```
ArgumentException: Event at index 2 is PicoActor.Abs.SagaCompleted, which is not a PicoAgent.Domain.DomainEvent
```

Two independent gaps hide behind it, and **both must be closed for the feature to work at all**:

1. **No registration**: `SagaCompleted`/`SagaFailed` carry no `[PicoDerivedType]`/`[GenerateSerializer]` anywhere
   (`grep src/PicoActor.Abs/` is empty), so no serializer exists for them process-wide.
2. **Generation is call-site driven**: PicoJetson's candidates are *invocations by method name*
   (`shared/Gen/GenInfrastructure.cs:1042-1069`) plus attribute markers on **type declarations**
   (`GenPipeline.cs:60-70`). A generic `Add<T>(tag)` generates nothing for `T` — so a design that only routes tags
   would fail on the **first write** with `InvalidOperationException: No serializer/deserializer registered for
   PicoActor.Abs.SagaCompleted`.

## Why an interface base (`TEventBase = IDomainEvent`) is not an option

`[PicoDerivedType]` is `AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)` — it cannot be placed on an
interface at all, and it may only name types that genuinely derive from the base. Registration by inheritance is the
coupling being broken; it cannot span an ownership boundary.

## Decision

### D1 — Bindings live on the store's options; the framework seeds its own events

```csharp
public sealed class EventTypeBindings
{
    public EventTypeBindings Add<T>(string tag) where T : class, IDomainEvent;   // routing only (see D2b)
    public int RegisteredCount { get; }        // every binding on this options instance
    public int FrameworkDefaultCount { get; }  // framework defaults APPLIED (0..2)
    internal bool TryAddDefault<T>(string tag) where T : class, IDomainEvent;    // TryAdd semantics
}

public sealed class JsonlEventStoreOptions
{
    public EventTypeBindings EventTypeBindings { get; } = new();   // EMPTY: one seeding mechanism only
}
```

**The store constructor is the only owner of the defaults** (no initializer seeding). It seeds once per options
instance with **TryAdd semantics** — a consumer that
called `options.EventTypeBindings.Add<SagaCompleted>("AcmeSagaDone")` keeps its tag (defaults never overwrite), and
`FrameworkDefaultCount` reports how many defaults were actually applied:

| Type | Canonical tag |
|---|---|
| `SagaCompleted` | `PicoActorSagaCompleted` |
| `SagaFailed` | `PicoActorSagaFailed` |

Both tags are the names a production corpus already carries (2,403 + 6 records): one tag per type in the wild, no
migration, no alias. They are permanent wire format from this release.

### D2 — Wire format unchanged; the discriminator is spliced in front

A bound event is written exactly like a polymorphic derived event, by a **byte-exact** recipe:

```
out = '{"$type":"' + tag + '"' + (payload.Length > 2 ? "," : "") + payload[1..]
// payload = SerializeToUtf8Bytes(concreteEvent), payload[0] == '{'
// SagaCompleted(null) -> {"Result":null}  ->  {"$type":"PicoActorSagaCompleted","Result":null}
// zero-member payload  -> {}              ->  {"$type":"<tag>"}
```

The **comma is conditional**: dropping the payload's opening brace leaves its first member name (`"Result":null}`),
so a non-empty payload needs `,` after the discriminator and an empty one (`{}` → `}`) must not get one. Both wrong
forms were measured: an unconditional comma yields `{"$type":"t",}` and no comma yields `{"$type":"t""Result":null}`.

The separator is emitted **only when the payload has a member** (`{"$type":"t","M":1}`) and never for a memberless payload (`{"$type":"t"}`) — the payload's opening brace is always dropped — `'{"$type":"<tag>",' + payload` yields
`{"$type":"t",{"Result":null}`, which is invalid JSON and would only surface on read-back (possibly after it reached an
archive). A tag is validated before it can be spliced (D1b).
A concrete-type deserializer tolerates the extra `$type` member (verified: the generated reader skips unknown members
unless `UnmappedMemberHandling.Disallow` is set, and both call sites pass no `JsonOptions`), so no payload rewriting and
the existing corpus stays byte-compatible.

**That tolerance is an invariant with a test**, not an accident: `Bnd3` pins one real, de-identified corpus line as a
byte literal (`{"$type":"PicoActorSagaCompleted","Result":null}`), loads it, appends the result into a fresh root, and
asserts the produced line is `SequenceEqual` to that literal. The line's writer was the consumer's own store (the only
component that ever produced such lines). Introducing `JsonOptions` on either call site must therefore be paired with an
`UnmappedMemberHandling` guard.

### D1b — `Add`/freeze semantics (the matrix, so two implementers cannot disagree)

| Call | Result |
|---|---|
| `Add<T>(tag)` — same type, same tag as an existing binding | idempotent no-op |
| `Add<T>(otherTag)` — same type, different tag | `InvalidOperationException` **at the call** |
| `Add<U>(tag)` — same tag, different type | `InvalidOperationException` **at the call** |
| tag empty / whitespace-only / contains `"`, `\` or a control character | `ArgumentOutOfRangeException` **at the call** |
| any `Add` after the first store construction over these options | `InvalidOperationException` (bindings are frozen) |

Freezing is atomic: the first construction takes a lock, copies the table and marks it frozen; a concurrent construction
blocks and then sees a frozen table (defaults are applied idempotently inside that same critical section, so two threads
constructing two stores over one options instance cannot double-seed). The public constructor of `EventTypeBindings` is
**internal**; the only public factory story is `new JsonlEventStoreOptions().EventTypeBindings`.

### D2b — Where the bound type's serializer comes from (the constraint this design must name)

`Add<T>(tag)` registers **routing only**; it does not generate a serializer. A bound type needs a generated serializer
in **some** assembly that is loaded in the process:

- **The framework's two events**: this package (which already references PicoJetson with `PrivateAssets="none"`) ships
  a marker type — `[GenerateSerializer(typeof(SagaCompleted))] [GenerateSerializer(typeof(SagaFailed))]
  internal static class FrameworkEventSerializers;` — so the generators run in `PicoActor.EventStore.Jsonl` itself and
  the registration is process-wide. Zero consumer wiring.

  **The attribute form is not usable for these two events** (measured, first implementation probe):
  `[GenerateSerializer(typeof(SagaCompleted))]` makes the generator emit `new SagaCompleted()` plus init-only property
  assignments — it takes the parameterless-constructor path because PicoSerDe's attribute pipeline extracts *members*
  and never reads a positional record's primary constructor (`CS7036: no argument given for required parameter
  'Result'`, `CS8852: Init-only property`). Reproduced with a record declared inside this very assembly, so it is a
  PicoSerDe limitation rather than a metadata effect (worth an upstream issue).

  The shipped mechanism is therefore the **closed call site**, which rev 3 already names as an accepted equivalent: a
  non-generic `internal static void Generate()` in `FrameworkEventSerializers` holds four literal invocations
  (`SerializeToUtf8Bytes`/`Deserialize` for both events) — Pipeline A generates for those, in this package. The same
  type owns the two tag constants (`FrameworkEventTags.SagaCompleted` / `.SagaFailed`), so the literals have exactly one owner; `FrameworkEventSerializers` aliases them.

  **The tripwire must not defeat itself**: any invocation named `Serialize`/`Deserialize`/… in the *test* assembly
  generates a registration there, so a naive `Bnd0` would pass with the generation point deleted. `Bnd0` therefore
  reaches the serializers through **method-group delegates** (`Func<SagaCompleted, JsonOptions?, byte[]> s =
  JsonSerializer.SerializeToUtf8Bytes<SagaCompleted>;` — a member access, not an invocation) and asserts the registry
  entries are non-null; deleting the generation point makes the delegate creation throw.

  **`Add<T>(tag)` never makes a type serializable** — it only makes an *already serializable* type writable and
  readable through this store. That sentence belongs in the public XML doc and the release notes.
- **A consumer's own bound type**: it must be generated in the consumer's assembly (a literal
  `Serialize<T>`/`Deserialize<T>` call, or a `[GenerateSerializer(typeof(T))]` marker on one of its types). This is
  documented, and the failure is loud (D5).

### D3 — Accepted set is `TEventBase ∪ bound types`; mixed batches are normal

`Encode` (today `private static`, so it becomes an instance method or takes the frozen table as a parameter) accepts an
event when it is a `TEventBase` **or** a bound type; the batch is still **one buffer, one
`WriteAsync`, one `FlushAsync`** (encode already happens before the file is created — no second write path). An event
that is neither is refused with the existing `ArgumentException`, whose message **keeps** the existing wording
(`PicoDerivedType`, base type name) and **appends** the binding remedy.

### D4 — The read path dispatches on the discriminator, which must be the FIRST member

New helper `TryReadDiscriminator(ReadOnlySpan<byte> line, out string tag)` — it inspect only the object's **first
member** (a prefix scan, never `IndexOf`, which would match a payload string) and matches case-insensitively, like the
polymorphic path. It also verifies the member's **name**: a library may declare a custom discriminator name
(`[PicoPolymorphic(TypeDiscriminatorPropertyName = …)]`), and such a stream must keep reading exactly as it does today.

**Therefore the rule is a fallback, not a gate**: if the first member is not a discriminator this store recognises, or
its value is not a bound tag, the line is deserialized through the **existing** `Deserialize<TEventBase>` path
unconditionally — "did not sniff" is never turned into a new error. Only a line that reaches the polymorphic reader and
is rejected there produces the existing `FormatException`.

Dispatch: tag found and bound → that binding's closed generic; otherwise → the existing `Deserialize<TEventBase>` path.
**Unchanged semantics, stated**: an unterminated tail line is still dropped as a torn tail with a Warning, and
`PeekFirstAsync` still swallows an undeserializable first line into `null` (so `IdOnly` enumeration still drops that id).

### D5 — Failure policy

| Situation | Outcome |
|---|---|
| write an event that is neither `TEventBase` nor bound | `ArgumentException` naming the index, the type, and `EventTypeBindings.Add<T>` |
| write a bound type that has **no generated serializer** | first write throws `InvalidOperationException` naming the type and the remedy (`[GenerateSerializer(typeof(T))]` in its own assembly); nothing is written, no file is created |
| read a line whose `$type` is neither bound nor a `TEventBase` discriminator | existing wrapped `FormatException`, listing the tag and the registered tags |
| read a line with no `$type` (or not first) | existing `FormatException` |
| two bindings with the same tag, or one type bound twice with different tags | `InvalidOperationException` **at the `Add` call** (the offending line fails, never a later construction) |
| a bound type assignable to `TEventBase` | `InvalidOperationException` at construction, **before the bindings are frozen** — so the options instance stays usable, and the message's only remedy is to delete that `Add` line |
| `TEventBase` is `IDomainEvent` (or another type the framework defaults are assignable to) | `InvalidOperationException` at construction: interface bases cannot carry `[PicoDerivedType]`; use a class base |
| `Add` after a store froze the bindings | `InvalidOperationException` naming the construction that froze them |

A line whose tag **is** bound but whose payload does not fit it is wrapped with a **binding-specific** message (the
tag, the bound type, the file, and where the tag was registered) — never the `Wrap` text that tells the reader to put
`[PicoDerivedType]` on `TEventBase`, which would be wrong advice for a bound event.

### D6 — Naming: a stream's prefix is its first event's CLR type name

Unchanged rule (`events[0].GetType().Name`): a saga stream whose first event is the completion is named
`SagaCompleted-<id>.jsonl`. A prefix is *only* that — **nothing about a prefix is a format invariant**. A corpus may
put framework-terminal-first streams under a non-type prefix (the reference corpus uses `unknown-<id>.jsonl`, 17
streams today); on such a stream the store must:

- **reachable by id** — the map learns `unknown` from the file name and `PathFor(id)` resolves it, so every read member
  works;
- **invisible to type enumeration** — `ListAggregateIdsAsync("SagaCompleted")` does not return it (prefix mismatch);
- **not a misconfiguration and not "foreign"** — it parses under the configured format, so it is `ConfiguredOtherType`:
  no `ForeignFormatFiles`, no Warning, no whole-directory throw.

**A prefix must not contain `-`** — `JsonlNames.TrySplitTypePrefix` splits at the **first** hyphen, so a type name with
one would make every stream of that type `UnparseableId` (counted, warned, un-enumerable). C# identifiers cannot contain
`-`, so this states the rule rather than adding a check. **Short-name uniqueness is by CLR short name** (two event types with the same short name share one prefix) — this is
pre-existing behaviour, not diagnosed here, and is stated so no consumer assumes otherwise. A tag never enters a file
name.

### D7 — Startup self-report

When at least one binding is registered, construction logs exactly one line:

```
event type bindings: {RegisteredCount} registered ({FrameworkDefaultCount} framework-default)
```

Because the framework seeds two defaults, this line is emitted for every `TypePrefix`… and every store: "when any
binding exists" is a tautology in practice and is written as such — the line always appears (with `0 registered` only
if a future build seeds nothing). `RegisteredCount`/`FrameworkDefaultCount` are public so a boot check can assert them
without parsing logs.

### D8 — Ownership: the constructor snapshots the bindings

A store **copies** the bindings into an immutable table at construction; later `Add` calls do not affect a live store,
and the append hot path reads a frozen structure (no mutable-dictionary reads under concurrency).
`JsonlEventStoreOptions` is a record, so its generated equality now includes the bindings reference: two options are
equal only when they share the same `EventTypeBindings` instance (documented, release-noted). One store per options
instance remains the supported shape.

### D9 — AOT

Routing and dispatch are closed generics captured by `Add<T>` (`SerializeToUtf8Bytes<T>` / `Deserialize<T>`), stored in
a frozen dictionary of delegates; no reflection and no runtime code generation. The write path does look up the runtime
`Type` in that read-only dictionary, and construction uses `IsAssignableFrom` once — neither is reflection. `IsAotCompatible`/IL2xxx stay
clean.

### D10 — `object?` members of framework events (fidelity table)

> **Superseded for 2026.11.2+.** PicoSerDe 2026.14.0 made an `object?` member reach the wire: `null`
> round-trips, `string`/`bool` verbatim, integers read back as `long`, and a runtime type the any-value
> chain cannot express throws before a byte is written. The table below describes **2026.11.0–2026.11.1**
> (PicoJetson 2026.13.3) and is kept as the record of why the tripwire test exists; the shipped contract
> is in `docs/release-notes/v2026.11.2.md`.

`SagaCompleted.Result` is `object?`. PicoJetson treats `object` as a dynamic member:

| Value written | Today's outcome (measured, PicoJetson 2026.13) |
|---|---|
| `null` | round-trips as `null` — **the only shape the framework itself produces** (every `MarkComplete` call site is parameterless) |
| anything non-null | **cannot be written or read by anyone**: the generated writer emits the member name with no value (`if (value.Result != null) { }`) → `FormatException`, and the reader assigns `default!` |

So the store does not promise string fidelity, and it cannot: the limitation is in the serializer, not in the binding.
The store's obligation is the one thing it controls — **a loud failure with nothing written** (`Bnd15` pins exactly
that, and goes red on purpose the day PicoJetson learns untyped members, at which point the test is extended into the
full round trip). The `SagaContracts` guidance ("keep it a simple serializable type") is therefore corrected to: for a
persistent store, `Result` must stay `null`.

## Non-goals (this release)

- Replacing `TEventBase` with a registry-only store (a bigger API break; D1 is the same capability, additive).
- Write-side aliases, tag renaming, or tag migration tools.
- `AddFilePrefixAlias<T>` — recused: an `unknown-*` corpus is readable by id without it (D6), so the API would exist only
  for a case that does not need it.
- Reading types the caller never bound.

## Testing (each red before green)

| Case | Asserts |
|---|---|
| `Bnd0_FrameworkEventSerializer_IsGenerated` | `SerializeToUtf8Bytes(new SagaCompleted(null))` does not throw (the package marker really produced a serializer) |
| `Bnd1_MixedBatch_RoundTrips` | `[business, SagaCompleted]` in one `AppendAsync`: `GetVersionAsync == 2`, exactly two lines in order, no torn-tail Warning (the store has no write counter — the observable triple is version + lines + clean log) |
| `Bnd2_SagaOnlyStream_PrefixIsTheTypeName` | first event `SagaCompleted` → `SagaCompleted-<id>.jsonl`; enumeration by that type finds it with zero opens |
| `Bnd3_ExistingCorpusTag_IsReadable_AndRewritesByteIdentical` | a real corpus-shaped line (`"$type":"PicoActorSagaCompleted"`) loads **and** appending then re-reading produces the byte-identical line |
| `Bnd4_UnknownTag_IsLoud` | `{"$type":"who-knows"}` → `FormatException` naming the tag and the registered tags |
| `Bnd5_UnboundEvent_IsRefused_WithTheRemedy` | an unbound, non-`TEventBase` event → `ArgumentException` keeping the old wording and naming `EventTypeBindings` |
| `Bnd6_DuplicateTag_ThrowsAtConstruction` | two `Add<T>` with the same tag → `InvalidOperationException` before any write |
| `Bnd7_BoundTypeAlsoTEventBase_ThrowsAtConstruction` | ambiguity refused |
| `Bnd8_CountsAndTheStartupLine` | `RegisteredCount == 2`, `FrameworkDefaultCount == 2`, exactly one Info line (requires the fixture to record levels — D11) |
| `Bnd9_BoundTypeWithoutGeneratedSerializer_IsRefusedOnFirstWrite` | a bound type with no marker/call → `InvalidOperationException` naming `[GenerateSerializer]`, and no file created |
| `Bnd10_IdOnly_And_TypePrefix_Agree` | both formats round-trip the same mixed batch |
| `Bnd11_UnknownPrefix_IsReachableById_NotByType` | `unknown-<id>.jsonl` carrying `"$type":"PicoActorSagaCompleted"`: `LoadAsync(id)` returns it; `ListAggregateIdsAsync("SagaCompleted")` does not; `ForeignFormatFiles == 0`; no Warning; no throw |
| `Bnd12_InterfaceBase_IsRefusedAtConstruction` | `JsonlEventStore<IDomainEvent>` → `InvalidOperationException` explaining that an interface cannot carry the registration |
| `Bnd13_NonFirstDiscriminator_IsTreatedAsAbsent` | `{"Result":null,"$type":"PicoActorSagaCompleted"}` → existing `FormatException` (documented) |
| `Bnd14_ConsumerOverrideWins` | `options.EventTypeBindings.Add<SagaCompleted>("Acme")` before construction: store uses `Acme`, `FrameworkDefaultCount == 1` |
| `Bnd15_NullResult_RoundTrips_NonNullFailsLoudly` | `Result = null` round-trips; `Result = "boom"` throws and writes **no file** (the serializer limitation, pinned so it changes loudly) |

### D11 — Test fixture requirement

`RecordingLogger` currently discards the level (`RecordingLogger.cs:29-34`); extend it to retain `(LogLevel, string)` so
`Bnd8` can assert the line is **Info**, not merely present.

## Repo surface

README ×10 (naming section: bindings, the two tags, mixed batches, the `unknown-*` rule, the serializer-source rule),
`picohex-actor` skill (one line), `docs/release-notes/v2026.11.0.md` (API delta, the record-equality/`ToString` change,
the `object?` fidelity table, migration sentence), and the rev-5 delta in `2026-11-16-jsonl-event-store-design.md`
(§Public surface + §Failure policy rows).

**API baseline delta** (`api/PicoActor.EventStore.Jsonl.public.txt`, refreshed by `release.ps1` — never pre-refreshed):
`T:PicoActor.EventStore.Jsonl.EventTypeBindings`, its `M:` rows for `Add`, `RegisteredCount`/`FrameworkDefaultCount`
properties, and `P:…JsonlEventStoreOptions.EventTypeBindings`. Only this package's baseline changes: the framework events
and their serializers stay in `PicoActor.Abs` untouched (no new dependency there).

## Self-review

1. **C1 closed by construction**: the serializer source is named (package marker for framework events; consumer rule
   for their own), and its absence has a test and a message (D2b, Bnd9).
2. **C2/C3 closed**: one seeding mechanism with TryAdd semantics; the store freezes bindings at construction; both have
   tests (Bnd14, and the frozen-table rule is observable through "an `Add` after construction does not change the store").
3. **I1–I8/M1–M5** each map to a paragraph or a test row; the alias API is removed rather than conditionally tested.
4. **Risk left**: short-name collision is stated, not diagnosed (I7), and the `object?` fidelity table documents lossy
   non-null payloads rather than preventing them.
