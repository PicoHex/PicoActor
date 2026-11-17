# Event-type bindings — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or executing-plans. Steps use checkbox syntax.

**Goal:** Let `JsonlEventStore<TEventBase>` write and read the framework's own `SagaCompleted`/`SagaFailed` (sealed, `IDomainEvent`, not deriving from `TEventBase`) through an explicit, pre-seeded binding table, without changing the wire format.

**Architecture:** `EventTypeBindings` (on the options) maps a bound event type to a wire tag; the store freezes it at construction, seeds two framework defaults with TryAdd semantics, and gains a byte-exact discriminator splice on the write path and a fallback-preserving discriminator sniff on the read path. The framework's serializers are generated **inside this package** by a `[GenerateSerializer]` marker class, because PicoJetson's generator is call-site driven and a generic `Add<T>` generates nothing.

**Tech Stack:** C# / net10.0, PicoJetson (source-generated), TUnit, PicoLog.Abs.

**Spec:** `docs/superpowers/specs/2026-11-18-event-type-bindings-design.md` (rev 3)

## Global Constraints

- `Add<T>(tag)` **routes only** — it never makes a type serializable. Same type + same tag is idempotent; same tag/different type and same type/different tag throw `InvalidOperationException` **at the call**; a tag that is empty, whitespace, or contains `"`, `\` or a control character throws `ArgumentOutOfRangeException` **at the call**; any `Add` after a store froze the table throws.
- The store **freezes** the bindings at construction (atomic: lock, copy, mark frozen); the append hot path reads the frozen table. Defaults are applied inside that same critical section with TryAdd (never overwrite a consumer binding).
- `EventTypeBindings`' public constructor is **internal**; the only public entry is `new JsonlEventStoreOptions().EventTypeBindings`.
- Write recipe is byte-exact: `'{"$type":"' + tag + '"' + payload[1..]` where `payload = SerializeToUtf8Bytes(concrete)` and `payload[0] == '{'`. **No comma, opening brace dropped.** Zero-member payload → `{"$type":"<tag>"}`.
- Read rule is a **fallback, not a gate**: sniff only the object's **first member**, case-insensitively; if its name is not the discriminator this store recognises or its value is not a bound tag, deserialize through the existing `Deserialize<TEventBase>` path unconditionally. Torn-tail and `PeekFirstAsync`-swallow semantics are unchanged.
- Accepted set is `TEventBase ∪ bound`; the batch stays one buffer, one `WriteAsync`, one `FlushAsync`. A bound type also assignable to `TEventBase`, or `TEventBase` being `IDomainEvent`, is refused at construction.
- Canonical tags: `PicoActorSagaCompleted`, `PicoActorSagaFailed` (permanent wire format; no aliases in this release).
- `TypePrefix` prefix = `events[0].GetType().Name`; a prefix never contains `-`; short-name uniqueness is by CLR short name (pre-existing, not diagnosed).
- Build 0 warnings; `dotnet csharpier check .`; explicit `git add`; do not pre-refresh `api/`.

## Review Focus

1. `Bnd0` — the package marker really produced serializers for both framework events (`SerializeToUtf8Bytes(new SagaCompleted(null))` must not throw).
2. `Bnd3` — a pinned corpus line round-trips **byte-identically** (`SequenceEqual`), proving the splice recipe.
3. `Bnd9` — a bound type with no generated serializer fails on first write with a message naming `[GenerateSerializer]`, and creates no file.
4. `Bnd11` — an `unknown-<id>.jsonl` stream is reachable by id, invisible to type enumeration, and neither "foreign" nor a misconfiguration.
5. `Bnd13` — a `$type` that is not the first member keeps today's behaviour (no new error, no silent mis-dispatch).

## File Structure

- `src/PicoActor.EventStore.Jsonl/EventTypeBindings.cs` (new)
- `src/PicoActor.EventStore.Jsonl/FrameworkEventSerializers.cs` (new: the marker; also the canonical tag literals)
- `src/PicoActor.EventStore.Jsonl/JsonlEventStoreOptions.cs` (property)
- `src/PicoActor.EventStore.Jsonl/JsonlEventStore.cs` (freeze + seed + `Encode` + `DeserializeLine` + `TryReadDiscriminator`)
- `src/PicoActor.EventStore.Jsonl/JsonlNames.cs` (discriminator helper, if it fits there)
- `tests/PicoActor.EventStore.Jsonl.Tests/EventTypeBindingTests.cs` (Bnd0–Bnd15)
- `tests/PicoActor.EventStore.Jsonl.Tests/RecordingLogger.cs` (record the level)
- docs: `README.md` ×10, `picohex-actor` skill, `docs/release-notes/v2026.11.0.md`, rev-5 delta

---

### Task B1: `EventTypeBindings` — the table, its grammar, and the freeze

**Files:** `src/PicoActor.EventStore.Jsonl/EventTypeBindings.cs`, `JsonlEventStoreOptions.cs`, `JsonlEventStore.cs` (seed + freeze only); `tests/.../EventTypeBindingTests.cs`.

- [ ] **Step 1: failing tests** — `Bnd6_DuplicateTag_ThrowsAtTheCall`, `Bnd7_SameTypeDifferentTag_ThrowsAtTheCall`, `Bnd14_ConsumerOverrideWins` (`Add<SagaCompleted>("Acme")` before construction → store uses `Acme`, `FrameworkDefaultCount == 1`), `Bnd16_TagGrammar_RejectsQuotesBackslashAndBlanks`, `Bnd17_AddAfterFreeze_Throws`.
- [ ] **Step 2: run** the filter → compile error.
- [ ] **Step 3: implement** the class (internal ctor, `Add`, `TryAddDefault`, `RegisteredCount`, `FrameworkDefaultCount`, freeze state machine), the options property, and the constructor's "freeze + seed with TryAdd" (single owner; no initializer seeding).
- [ ] **Step 4: run** → green; full Jsonl suite green; format; commit `feat(store): add the event type binding table`.

### Task B2: the framework's serializers live in this package

**Files:** `src/PicoActor.EventStore.Jsonl/FrameworkEventSerializers.cs`; test `Bnd0`.

- [ ] **Step 1: failing test** — `Bnd0_FrameworkEventSerializer_IsGenerated` calls `JsonSerializer.SerializeToUtf8Bytes(new SagaCompleted(null))` and asserts it does not throw and deserializes back.
- [ ] **Step 2: run** → `InvalidOperationException: No serializer/deserializer registered…`.
- [ ] **Step 3: implement** the marker class carrying `[GenerateSerializer(typeof(SagaCompleted))]` and `[GenerateSerializer(typeof(SagaFailed))]`, plus the two canonical tag constants (one owner for the literals).
- [ ] **Step 4: run** → green; commit `feat(store): generate the framework event serializers in the store package`.

### Task B3: the write path — accepted set and the byte-exact splice

**Files:** `JsonlEventStore.cs` (`Encode` becomes an instance method reading the frozen table), tests.

- [ ] **Step 1: failing tests** — `Bnd1_MixedBatch_RoundTrips` (version 2, two lines in order, no torn-tail warning), `Bnd2_SagaOnlyStream_PrefixIsTheTypeName` (`SagaCompleted-<id>.jsonl`, zero opens on enumeration), `Bnd5_UnboundEvent_IsRefused_WithTheRemedy` (keeps the old wording and appends `EventTypeBindings`), `Bnd10_IdOnly_And_TypePrefix_Agree`, `Bnd15_NullAndStringResult_RoundTrip`.
- [ ] **Step 2: run** → red (`which is not a <TEventBase>`).
- [ ] **Step 3: implement** the accepted-set check (`is TEventBase` → existing path; else bound → splice; else the existing `ArgumentException` with the appended remedy) inside the single-buffer `Encode`.
- [ ] **Step 4: run** → green; commit `feat(store): accept bound events in a batch`.

### Task B4: the read path — discriminator sniff with an unconditional fallback

**Files:** `JsonlEventStore.cs` (`DeserializeLine`, `TryReadDiscriminator`), tests.

- [ ] **Step 1: failing tests** — `Bnd3_ExistingCorpusTag_IsReadable_AndRewritesByteIdentical`, `Bnd4_UnknownTag_IsLoud`, `Bnd9_BoundTypeWithoutGeneratedSerializer_IsRefusedOnFirstWrite`, `Bnd11_UnknownPrefix_IsReachableById_NotByType`, `Bnd12_InterfaceBase_IsRefusedAtConstruction`, `Bnd13_NonFirstDiscriminator_IsTreatedAsAbsent`.
- [ ] **Step 2: run** → red.
- [ ] **Step 3: implement** the first-member, case-insensitive sniff + bound dispatch + fallback; the binding-specific wrap message; the construction-time refusals (`IDomainEvent` base, assignable bound type).
- [ ] **Step 4: run** → green; full suite; format; commit `feat(store): read bound events by discriminator`.
- [ ] **Step 5: reverse-break (manual, revert each)**: trust the miss/tag without the first-member check → Bnd13 red; drop the splice's `payload[1..]` brace handling → Bnd3 red; remove the `[GenerateSerializer]` markers → Bnd0/Bnd1 red.

### Task B5: docs and release 2026.11.0

- [ ] README ×10 (bindings, the two tags, mixed batches, the `unknown-*` rule, "`Add` routes, it does not make a type serializable"), skill one line, `docs/release-notes/v2026.11.0.md` (API delta, record-equality/`ToString` change, the `object?` fidelity table, the serializer-source rule, migration), rev-5 delta.
- [ ] `perf`-style verification not needed; run the five suites; `release.ps1 -DryRun` must report **API changed → v2026.11.0**; then `-Push`; watch Release + CI; verify the nupkg.

## Self-Review

1. **Spec coverage:** D1/D1b → B1; D2b → B2; D2/D3 → B3; D4/D5/D6 → B4; D7/D10 + repo surface → B1 (counts) and B5.
2. **Test coverage:** every Bnd case has an owning task and an assertion.
3. **Type consistency:** `EventTypeBindings`, `Add`, `TryAddDefault`, `RegisteredCount`, `FrameworkDefaultCount`, `TryReadDiscriminator` — used identically across tasks.
4. **Proportion:** ≈120 lines of plan against ≈290 lines of spec; no implementation bodies.
