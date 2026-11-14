# Snapshot payload schema envelope — design (rev 3)

Status: revised after two independent reviews (2026-11-14): rev 2 closed the
envelope-boundary review; rev 3 closes the spec review (placement, out-param
semantics, missing acceptance, unfalsifiable mutation check, byte-exact assertions,
rollout sequencing, instance-tag hazard, baseline/release conflict).

Rev 3 changes: the unwrap location is stated (and must not touch `ActorSystem`);
`TryUnwrapSnapshot`'s no-envelope out-values are fixed; a 13th/14th test covers the
write-path throw and a double rebuild; test 1 asserts the payload byte-for-byte; the
rollout is a two-deploy sequence; the tag is a type-level decision; the API baseline is
never pre-refreshed.

## Intent

Snapshots are an acceleration only: history is never truncated, so a snapshot may
always be discarded and the stream replayed in full. The failure mode this design
targets is applying a payload produced by an older state shape and silently getting a
wrong aggregate. Where a tag exists, the framework decides applicability; an untagged
payload is still validated by the actor's own `RestoreSnapshot`.

Two alternatives were rejected on purpose:

- A `string? Schema` field on `ActorSnapshot` — it changes the store contract: every
  store, including file stores outside this repository, would need a new column and a
  migration.
- Actor-side self-validation — a shape change that keeps the byte layout (an `int` whose
  meaning changed) is undetectable from the payload alone: the actor can prove “these
  bytes look like mine”, never “these bytes belong to my current shape”.

Deployments do change actor state shapes while old snapshots remain on disk, so the
check is mandatory for actors that opt in.

## Format (framework-owned, inside the opaque payload)

```
offset 0..3  'P','A','S','1'      magic, 4 bytes
offset 4     schemaLen            1 byte, 0..255 (UTF-8 BYTE count)
offset 5..   schema               UTF-8 bytes
rest         actor payload        exactly what TakeSnapshot() returned
```

- Written only when the actor declares a schema (`SnapshotSchema is not null`);
  otherwise the payload is stored byte-for-byte as before.
- `ActorSnapshot` and `IEventStoreSnapshot` are unchanged: stores keep treating the
  payload as opaque bytes, so every store (including file stores outside this
  repository) gets the protection with zero code changes.
- The first four bytes never change for a future format revision; any evolution goes
  inside the schema tag (e.g. `CounterState/v2`). `PAS1` is the container version.

### Reading an envelope (fixed order, do not reorder)

1. `payload.Length < 5` **or** `payload[0..4] != 'P','A','S','1'`
   → **no envelope**: `TryUnwrapSnapshot` returns `false` with `actorPayload = payload`
   (the whole span, never `default`) and `schema = null`. The caller applies the whole
   span **when the tag is null; a declared-tag actor rejects it instead — see the matrix**.
2. magic matches, but `payload.Length < 5 + payload[4]`
   → **corrupt envelope**: throw `InvalidOperationException(
   $"snapshot envelope is truncated: schemaLen={payload[4]}, payload={payload.Length}")`.
   (The magic is *assumed* to mean the framework wrote the header; a legacy payload that
   starts with the magic is indistinguishable and lands here — see risk 3.)
3. otherwise decode `payload[5 .. 5+schemaLen)` as UTF-8 with the **non-throwing**
   default semantics (`Encoding.UTF8` replaces invalid sequences with U+FFFD; it must not
   throw `DecoderFallbackException`), and compare with
   `string.Equals(stored, current, StringComparison.Ordinal)`.

### Where this lives

The unwrap lives in `EventSourcedActor`'s explicit
`ISnapshotableActor.RestoreSnapshot(ulong, byte[])` implementation
(`EventSourcedActor.cs:501`): it reads the protected tag, calls
`TryUnwrapSnapshot(payload, out actorPayload, out schema)` and passes the slice to the
protected `RestoreSnapshot(ulong, ReadOnlySpan<byte>)`. `ActorSystem`
(`ActorSystem.cs:409-430`) is NOT touched — that is why the public API delta is exactly
one property. The helper lives in `PicoActor.Abs` as a **private static** member: a
public/protected helper would contradict the one-property delta; the tests reach it through the
public `ISnapshotableActor` interface.

## API

```csharp
// State shape changed? Bump the tag. Changing it invalidates older snapshots.
protected virtual string? SnapshotSchema => null;
```

- **`null` (only `null`) disables the envelope.** An empty tag (`""`) is a real tag:
  `schemaLen = 0`. Both sides test `is null`; `string.IsNullOrEmpty` is wrong and creates
  an endless “write raw → read rejects → full replay” loop.
- The tag must be a property of the **type**, not of per-instance injected state: the
  value read by the rebuild instance must equal the value the writing instance produced.
  If it comes from a field, both the create factory and the rebuild factory must set it
  identically — a mismatch silently turns every recovery into a full replay, with only a
  warning as evidence.
- `ISnapshotableActor` implementations outside `EventSourcedActor` would have to unwrap
  by hand (none exist in this repository). Say so in the property's XML doc.

## Applicability matrix (restore)

The event-version guard (`0 < version <= events.Count`) is evaluated by
`ActorSystem.RestoreOrReplayAsync` **before** the envelope check; tests must not build
version-0 snapshots (they would pass because of that guard, not the envelope).

| Snapshot | Actor's `SnapshotSchema` | Decision |
|---|---|---|
| envelope, same tag | non-null | **apply**: `RestoreSnapshot(version, actorPayloadSlice)` + replay tail |
| envelope, different tag (incl. actor now `null`) | any | **reject**: full replay |
| envelope, truncated | any | **reject**: full replay (`InvalidOperationException` above) |
| no envelope | `null` | **apply** the whole payload (backward compatible) |
| no envelope | non-null | **reject**: full replay (cannot prove the bytes match) |

A rejection throws from `ISnapshotableActor.RestoreSnapshot`; the existing
`ActorSystem.RestoreOrReplayAsync` catch logs a warning and replays the whole stream.
A rejected snapshot is not deleted (`IEventStoreSnapshot` has no delete): it is
overwritten by the next snapshot, i.e. **after the aggregate has flushed another
`SnapshotEveryNEvents` events**. Low-traffic aggregates therefore replay in full on
every rebuild until they cross the threshold again.

Rejection messages:

- mismatch: `snapshot schema mismatch: stored '{stored}' vs current '{current}'.`
  (render `current` as `<null>` when the tag is null);
- untagged with a declared tag: `unversioned snapshot ignored because the actor declares
  schema '{current}'.`;
- truncated: the exact string in step 2 above.

## Write path (order is part of the contract)

```
TakeSnapshot()
  -> null? return (no snapshot, and NOT an OnSnapshotFailed — opting out is not a failure)
  -> open the failure scope: any exception below ends in ReportSnapshotFailure(ex) + return
  -> read SnapshotSchema + UTF-8 GetByteCount/GetBytes
  -> null? no envelope: save the payload as-is
  -> >255 bytes? ReportSnapshotFailure(new InvalidOperationException(...255...)) + return
  -> wrap into a NEW array (never mutate the buffer TakeSnapshot returned)
  -> SaveSnapshotAsync(new ActorSnapshot(Version, wrapped), StopToken)
```

- The schema read, byte count and encode **must not escape** `SaveSnapshotIfDueAsync`:
  a throwing `SnapshotSchema` override would otherwise turn an already-persisted,
  already-mutated command into a failed `AskAsync` (caller retries → duplicate command).
- An oversized tag is a **build-time defect**: it is a type constant, so snapshots stay
  off for that type and the failure repeats at every threshold — expect the log noise, it
  is not transient. A tag containing a lone surrogate never round-trips (`GetBytes`
  replaces it), so it will always mismatch; keep tags plain ASCII/UTF-8 text.
- Cost: one O(payload) allocation per threshold. For a 24 MB payload that is a 24 MB peak,
  amortized over N events — acceptable.

## Read path (zero-copy, read-only)

- `TryUnwrapSnapshot(ReadOnlySpan<byte> payload, out ReadOnlySpan<byte> actorPayload,
  out string? schema)` returns `true` only when an envelope is present; in every other
  case it returns `false` with `actorPayload = payload` (the whole span) and
  `schema = null`.
- The returned slice is a **window** (`payload[(5 + schemaLen)..]`). No copy.
- The input array belongs to the store and is read-only (`ActorSnapshot` contract); never
  shift bytes in place — that would permanently rewrite the stored snapshot and turn it
  into “no envelope” on the next read (observable only by rebuilding the same id twice).
- `RestoreSnapshot(ulong, ReadOnlySpan<byte>)` already takes a span, so no extra copy is
  needed.

## Failure behaviour (exact types/messages)

| Situation | Behaviour |
|---|---|
| `TakeSnapshot()` returns `null` | no snapshot; no `OnSnapshotFailed` |
| `TakeSnapshot()` throws | `OnSnapshotFailed(ex)`; command unaffected |
| `SnapshotSchema` read/encode throws | `OnSnapshotFailed(ex)`; command unaffected |
| schema > 255 UTF-8 bytes | skip + `OnSnapshotFailed(InvalidOperationException, message contains "255")`; repeats every threshold |
| store throws / cancels | `OnSnapshotFailed(ex)`; command unaffected |
| truncated envelope | `InvalidOperationException` (exact message above) → full replay |
| schema mismatch | `InvalidOperationException` (exact message above) → full replay |
| no envelope, actor declares a schema | `InvalidOperationException` (exact message above) → full replay |

## Goals

- Never silently apply a payload whose tag does not match.
- Zero change to the store contract and to `ActorSnapshot`.
- Zero change for actors that do not declare a schema.
- One slow rebuild **immediately after** a schema change; normal speed returns once the
  aggregate flushes another `SnapshotEveryNEvents` events (not unconditionally).

## Constraints

- Single TFM `net10.0` unchanged; no new package reference (`System.Text.Encoding` is
  BCL); no reflection, so the AOT/trimming tier is unaffected. (`System.Text` must be
  added to `src/PicoActor.Abs/GlobalUsings.cs` because the repo keeps zero file-level
  usings — plan detail.)
- History never truncated; one snapshot per aggregate; event-count trigger unchanged.
- `ActorSnapshot`, `IEventStoreSnapshot`, `InMemoryEventStore`, `ActorSystem` unchanged.

## Residual risks / rollout

1. **No tag, no protection.** `SnapshotSchema` defaults to `null`, so an actor that does
   not opt in keeps today's behaviour (payload applied as-is).
2. **Rollback / mixed versions — two deploys are required.** The envelope is inert until
   an actor declares a tag. The safe sequence over one store is: (1) ship the
   envelope-capable binary with `SnapshotSchema` still `null` on **every** node and
   confirm full rollout; (2) only then declare the tag. A single deploy that both ships
   the reader and declares the tag cannot be rolled back safely, because every old node
   reads a new enveloped payload as raw bytes **without an exception** (e.g.
   `BitConverter.ToInt32` returns `PAS1` as a number).
3. **Legacy payload beginning with `PAS1`.** A raw payload that happens to start with the
   magic is read as an envelope. A colliding payload shorter than 5 bytes takes the
   no-envelope path instead (applied by a null-schema actor); collisions of 5+ bytes are
   rejected — as truncated when the length byte exceeds the remaining bytes, as a tag
   mismatch otherwise. For a `null`-schema actor that is a rejection on every rebuild with
   no self-heal (its own new snapshots are raw too). Declaring a schema is the way out: the
   first enveloped snapshot then matches.
4. **Future format revisions keep the first four bytes.** New fields go inside the schema
   tag, never into the magic, so an old binary always sees “no envelope” (safe) rather
   than a half-understood one.

## Non-goals

- Snapshot retention/truncation (history stays complete).
- Keeping more than one snapshot (a schema mismatch invalidates all of them; corruption —
  bytes that are neither a valid envelope nor the current shape — is not diagnosed here
  and is out of scope).
- Byte-based trigger (the trigger stays event-count based).
- Payload compression or a framework serializer (encoding remains the actor's choice).
- Deleting stale snapshots (the store contract has no delete; the next snapshot
  overwrites).

## Testing (16 cases, all deterministic, no log-only assertions)

Actor: a counting ES actor whose payload is the current value (4 bytes), with a settable
`Tag` (`SnapshotSchema => Tag`); `SnapshotEveryNEvents = 5` unless stated.

1. `Envelope_IsWritten_WithUtf8ByteLength` — tag `"订单-v1"` (9 UTF-8 bytes; do not
   hard-code the number, use `GetByteCount`): the payload equals **exactly**
   `[PAS1][len][tag][actorPayload]` — magic, length, tag bytes, and the remaining bytes
   byte-for-byte equal to what `TakeSnapshot()` returned; nothing else.
2. `Envelope_IsApplied_ReplayingOnlyTheTail` — 12 events, snapshot at 10 → rebuild
   `MutateCalls == 2`, value correct.
3. `ChangedTag_IsRejected_AndFullReplay_ThenSelfHealsAtNextThreshold` — v1 snapshot, v2
   instance → full replay (`MutateCalls == total`); after the next threshold the new
   snapshot carries `v2`.
4. `LegacyRawPayload_WithDeclaredSchema_IsRejected_FullReplay` — a valid raw payload for
   that version, written via `SaveSnapshotAsync`; declared schema → full replay
   (`MutateCalls == total`, correct value).
5. `LegacyRawPayload_WithNullSchema_IsApplied` — the same raw payload with `Tag = null` →
   tail replay only.
6. `OversizedTag_SkipsSnapshot_ReportsOnce_CommandSucceeds` — 100 × `汉` (300 bytes),
   threshold 12 (one trigger) → no snapshot, command succeeds, `OnSnapshotFailed` exactly
   once.
7. `NullSchema_WritesRawPayload_Unchanged` — payload has no `PAS1` prefix and equals the
   actor payload byte-for-byte.
8. `Envelope_WithNullSchema_IsRejected` — written with `v1`, rebuilt with `Tag = null` →
   full replay (then self-heal with untagged snapshots at the next threshold).
9. `ThrowingSnapshotSchema_DoesNotFailTheCommand` — the override throws → command
   succeeds, no snapshot, `OnSnapshotFailed` receives it.
10. `TruncatedEnvelope_IsRejected_WithInvalidOperationException` — a direct
    `((ISnapshotableActor)new SchemaCounter()).RestoreSnapshot(10, [PAS1][9][1,2])` throws an
    `InvalidOperationException` whose message contains `truncated`; end-to-end, storing that payload
    and rebuilding yields a full replay (`MutateCalls == 12`, value 11). No log-only assertion.
11. `EmptyTag_IsARealTag_WrittenWithZeroLength` — `Tag = ""` → total payload length is
    exactly `5 + actorPayload.Length`, `payload[4] == 0`; rebuild applies it.
12. `LegacyPayloadStartingWithMagic_IsRejected` — a raw payload beginning with `PAS1` →
    rejected, full replay, actor usable (the documented collision cost).
13. `ThrowingTakeSnapshot_DoesNotFailTheCommand` — `TakeSnapshot` throws; the command's
    `AskAsync` succeeds, no snapshot is stored, `OnSnapshotFailed` received that exact
    exception instance.
14. `Envelope_IsRestoredTwiceFromTheSameStoredSnapshot` — rebuild the same id twice from
    one stored snapshot without an intervening write; both rebuilds tail-replay and the
    payload returned by `LoadSnapshotAsync` is byte-identical before and after (this is
    what makes “mutating the store's array” observable).
15. `TagOf255Bytes_IsWritten` — a 255-byte tag is the last one that fits: `payload[4] == 255`,
    total length `5 + 255 + payload.Length`, the tag bytes are exact, and the read side
    restores it (rebuild replays zero tail events).
16. `TagOf256Bytes_SkipsSnapshot` — one byte over: no snapshot, `OnSnapshotFailed` exactly once,
    command still usable.

Mutation checks (each must turn the named test red): delete the tag comparison (3/8);
treat `""` as `null` (11); move the schema read outside the failure scope (9); drop the try around `TakeSnapshot` (13); `> 255` → `>= 255` (15);
**mutate the store's array instead of slicing** (14 — a copy is behaviourally identical
and is not a mutation check); truncate instead of skipping an oversized tag (6).

## Documentation (surface only; details belong to the plan)

- `README.md` + 9 translations: the Snapshot section gains `SnapshotSchema`, “changing it
  invalidates old snapshots once”, “`null` = no protection”, and the envelope layout.
- `picohex-actor` skill: §Snapshot acceleration + a Common Mistakes row (“changed the state
  shape without bumping `SnapshotSchema`”).
- `docs/release-notes/v2026.7.0.md`: follow the existing structure (Why / Content / API /
  wire / breaking / Limitations left open); this is an **x** bump, so downstream consumers
  must migrate their float band.
- **Do not pre-refresh `api/PicoActor.Abs.public.txt`**: `scripts/release.ps1` refreshes
  it at apply time; a baseline already refreshed before `-DryRun` makes the tool answer
  “API unchanged” (y bump) where an **x** bump is due.

## Compatibility

- Existing snapshots (no envelope) keep working for actors without a schema.
- Adopting a schema intentionally invalidates old snapshots (one full replay, then
  threshold-gated self-heal).
- Public API delta: `EventSourcedActor.SnapshotSchema` (protected virtual) only.
