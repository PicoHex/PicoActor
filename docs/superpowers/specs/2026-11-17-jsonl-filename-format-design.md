# JSONL stream file naming — `FileNameFormat` design (rev 3)

Status: revised after two spec reviews (2026-11-17). Rev 3 closes N1–N10 from round 2 (stem parsing,
five-bucket classification, FileInfo-versus-File.Exists, all-entry enumeration, non-amortizable new-id
pass, lazy first-append pass, empty-batch rules, delta/counters). Rev 2 closed C1–C3 and I1–I12:
the id parser is a prefix **slice** (not `LastIndexOf`), a whole-directory misconfiguration throws
instead of enumerating nothing, the duplicate guard costs nothing on pure single-format
directories, snapshots are keyed by id in both formats, and the member-level undefined branches
are pinned.

## Problem

`PicoActor.EventStore.Jsonl` names a stream `<id:D>.jsonl`, so
`ListAggregateIdsAsync(firstEventType)` opens every file and deserializes its first line
(4,638 opens ≈ 452–517 ms per type scan). PicoAgent's stores name streams
`{firstEventType}-{id}.jsonl`, which makes the same call a filename scan (≈ 3.3 ms, zero opens).
Both stores share the content format; only the name differs, so an opt-in naming mode lets
PicoAgent migrate without renaming 4,654 files, without coordinating a stop-the-world migration,
and without a reversible-rename risk.

Two honest corrections to the ask: `File.Move` is a metadata operation (the 1.5 GB is not copied),
and the boot saving applies to **every type scan whose streams do not match the requested type**
(a type scan that matches still pays one `PeekFirstAsync` per id in `FindAggregateIds`).

The decisive reason a rename is not acceptable is outside this repository: PicoAgent's ledger,
archive, checkpoint and fact scans are **name-based consumers** of those files (rev 5 §Intent), so
a rename breaks them in the same change and needs coordinated downtime. `TypePrefix` needs
neither.

## API (additive; default preserves today byte-for-byte)

```csharp
public enum FileNameFormat
{
    IdOnly = 0,      // "<id:D>.jsonl"         — today's behaviour (default)
    TypePrefix = 1,  // "<type>-<id:D>.jsonl"  — first event's concrete type name
}

public sealed record JsonlEventStoreOptions
{
    // existing members unchanged
    public FileNameFormat FileNameFormat { get; init; } = FileNameFormat.IdOnly;
}
```

- The constructor throws `ArgumentOutOfRangeException(nameof(JsonlEventStoreOptions.FileNameFormat))`
  for an undefined value (a config typo must not silently fall back to `IdOnly`).
- `{type}` is `events[0].GetType().Name` of the first persisted batch — the same runtime
  `Type.Name` `ListAggregateIdsAsync(firstEventType)` matches today. Generic types keep their
  backtick arity (`Foo\`1`), exactly as `Type.Name` does; do not strip or use `FullName`.
- `IdOnly` is byte-identical: file names, enumeration results, counts.

## The hard part: `IEventStore` only carries the id

`AppendAsync(id)`, `LoadAsync(id)`, `OpenAsync(id, …)`, `GetVersionAsync(id)`,
`PeekFirstAsync(id)` and the snapshot members receive **only the aggregate id**. Under
`TypePrefix` the type is not derivable from the id, so the store keeps an
**`id → type` map** (`ConcurrentDictionary<Guid, string>`):

- **Built only when `FileNameFormat == TypePrefix`**: one filename scan of the root
  (`*.jsonl`, no file opens). A missing root yields an empty map — no throw, no directory
  creation. `IdOnly` performs **no** scan at construction.
- A new id enters the map when its first `AppendAsync` succeeds, from
  `events[0].GetType().Name`.
- On a miss, one **root filename pass** resolves the id (`*-{id}.jsonl`) and, while it is there,
  refreshes the map for every parseable name (a cold start costs one pass, not one per id).
- The map is bounded by the number of streams (one short string per stream, ~40 B); it is never
  evicted, because eviction would force a rescan on the next append. Two consequences are
  deliberate: after an operator deletes a stream file, the map entry survives and a later append
  recreates the file under the **old** prefix (same behaviour as `IdOnly`); and if an id is
  recreated with a different first event type, the old prefix is kept permanently — this is the
  price of "prefix fixed at creation" and is **not** caught (only two coexisting prefix files are
  caught by the §Mixed directories rule).
- **Rejected alternatives**: (a) per-operation `*-{id}` glob — 1–3 ms on the append hot path;
  (b) an `id → type` sidecar/index file — a second file to keep crash-consistent, and the content
  format is frozen; (c) an extra column in the stream header — changes the wire format rev 5
  fixed.

### Cost model (per operation)

| Situation | Cost |
|---|---|
| map hit | `TypePrefix`: 2 `FileInfo` stats (existence + other-layout); `IdOnly`: 1 stat |
| miss, the stream exists, `TypePrefix` | one root name pass; the pass completes the map for **every** parseable name (amortized across the ids it finds) |
| miss, a **new** id, `TypePrefix` | one root name pass, **not amortizable** — the id is not in the directory yet, so every new aggregate pays one pass on its first append (measured separately, N6) |
| miss, `IdOnly` | 0 per append; **one lazy root pass at the first observation of a missing stream (any stream member)**, latched for the instance (N2) — covers read-only hosts too |
| read of an **unknown** id under `TypePrefix` | one root pass **per call** (no negative cache) — the accepted cost of never caching a "does not exist" answer |

## Mixed directories (migration)

Exactly one format is authoritative — the one in `Options.FileNameFormat`. A scan classifies
every **directory file** into one of five buckets (sub-directories are not scanned; `DirectoryInfo.EnumerateFiles` skips them):

| Bucket | Test | Counted | Where | Log |
|---|---|---|---|---|
| **matches this query** | configured format, prefix == `firstEventType`, id parses | — | — | — |
| configured format, **another type** | configured format, prefix != `firstEventType`, id parses | **not counted, no log** | — | — |
| other format | valid under the **other** rule, **both directions**: under `IdOnly` a `{type}-{id}` stem (split at its **first** `-`, never the last), and under `TypePrefix` a bare `D` GUID stem — so a store pointed at the other layout is caught, not silently empty | `ForeignFormatFiles` | internal counter | one **Warning** per scan (`N files use the other naming format`) |
| foreign file (README, backups, non-`.jsonl`) | neither rule parses | `SkippedFileNames` | existing counter (per file **per pass**) | one Warning per file (existing behaviour) |
| configured format, id unparseable | prefix matches, suffix not a `D` GUID | `SkippedFileNames` | existing counter | one Warning per entry |

- **Whole-directory misconfiguration throws**: if **any** scan (enumeration, the `TypePrefix`
  constructor pass, or the lazy first-append pass) finds **zero** configured-format streams and
  **more than zero** other-format streams, it throws `InvalidOperationException` naming both
  formats and both counts, with the remedy ("migrate the directory or set `FileNameFormat`").
  This is the only loud outcome that reaches an operator who pointed an `IdOnly` store at a
  `{type}-{id}` directory — without it the store starts, enumerates nothing and the data is
  invisible. A store that never enumerates still gets the check on its **first observation of a missing
  stream** (the lazy pass — a stream load counts, not only an append; snapshot members do not participate), so the "I only load/append
  by id" host cannot silently fork.
- **Instance-level latches**: `ForeignFormatFiles > 0`, a latched lazy-pass failure and the
  conflicting-prefix set are never reset; repairing the directory takes a new store instance.
- **Duplicate guard, on demand**: the other-format check runs in `AppendAsync` **only after a scan
  (enumeration, constructor or lazy first-append) observed a `ForeignFormatFiles > 0`** (flag set
  by that scan; pure single-format directories pay only the one lazy pass). When it runs, a
  **non-empty** file for the same id in the other format makes the append throw
  `InvalidOperationException` naming both paths. A **zero-length** file is not a stream (rev 5
  allows a failed append to leave one) and is ignored — the check is
  `new FileInfo(path) is { Exists: true, Length: > 0 }`, **never** `File.Exists` alone.
  Under `TypePrefix` the check is that direct `FileInfo` test on `<id>.jsonl`; under `IdOnly` it is
  the `ForeignFormatFiles`-gated scan.
- **Empty batches** do the same path resolution (map → root pass) but skip the duplicate guard
  (they write nothing); their version check still runs, so a held stream's `expectedVersion` is
  validated in both formats.

## Enumeration (the point of the change)

`ListAggregateIdsAsync(firstEventType)`:

| Format | Implementation | Opens |
|---|---|---|
| `IdOnly` | enumerate **all directory entries**, `PeekFirstAsync` each `*.jsonl` stream (today) | one per stream |
| `TypePrefix` | enumerate all entries, keep `*.jsonl` names whose stem is `{firstEventType}-{id}`, parse the id | **zero** |

- The scan enumerates **all directory entries**, not `*.jsonl`: a non-`.jsonl` entry is a foreign
  file and is counted as skipped (today's behaviour, and the only way a stray README is reported).
- Parsing **strips the stream suffix first**, then slices the prefix (never searches for a
  separator): `stem = name[..^StreamSuffix.Length]` **only after** `name.EndsWith(StreamSuffix, Ordinal)`
  (the current implementation's guard; without it a short foreign name such as `x.txt` makes
  `name[..^6]` throw); `stem.Length > firstEventType.Length &&
  stem.StartsWith(firstEventType, Ordinal) && stem[firstEventType.Length] == '-'` →
  `Guid.TryParseExact(stem.AsSpan(firstEventType.Length + 1), "D", out id)`. (A GUID contains four
  `-`; `LastIndexOf('-')` would return the last 12 hex digits; `FileInfo.Name` still carries
  `.jsonl`.)
- The comparison is `StringComparison.Ordinal`, not the OS glob (case-insensitive on Windows).
- The scan uses `DirectoryInfo.EnumerateFiles`, whose `FileInfo.Length` comes from directory
  metadata (no read handle): a **zero-length** file is skipped, preserving `IdOnly`'s "an empty
  file is not a stream" rule. `FilesOpened` stays 0.
- **Accepted deviation**: a stream whose **first line is corrupt** is enumerated by `TypePrefix`
  (its name is valid) and not by `IdOnly` (which cannot read the first event). The runtime's
  `FindAggregateIds` peeks each returned id and filters the corrupt one; this is documented, not
  silent.

## Semantics that must not change

`GetVersionAsync`, `OpenAsync` (positioning, `End`, `cap`), `LoadAsync`, `PeekFirstAsync` and the
snapshot members keep their v2026.9.0 semantics in both formats; the format only changes which
path the resolver returns. Torn-tail/corruption rules, the `PASJ` header, flush/rename atomicity,
the bounded lock table and the counters are unchanged.

### Member semantics for the new branches

| Member | Unknown id (no file, no map entry) | Empty batch |
|---|---|---|
| `GetVersionAsync` | 0 | 0 |
| `LoadAsync` | empty list | — |
| `OpenAsync(id, fromVersion)` | empty cursor with `End == 0`; `fromVersion == 0` still throws | — |
| `PeekFirstAsync` | null | — |
| `LoadSnapshotAsync` | null | — |
| `SaveSnapshotAsync` | writes `<id>.snap` (no type needed — see below) | — |
| `AppendAsync` | expectedVersion must be 0 (else `ConcurrencyException`); a 0 event batch is a no-op that creates nothing | same path resolution (map → root pass), version check, **no duplicate guard** (writes nothing) |

**Append order** (it changes because `Encode` needs the path): `events.Count == 0`? → version
check via the map/path (no creation) → no-op, return; otherwise
`type = events[0].GetType().Name` → resolve path (map hit → `TypePrefix` root pass → new name) →
per-aggregate gate → witness → version → truncate → one `WriteAsync` → `FlushAsync(None)` →
cache + map update. A new id never produces a path like `-<guid>.jsonl` (the type is known before
the path is formed).

## Snapshots

**The sidecar is `<id:D>.snap` in both formats.** A snapshot's key is the aggregate id; the
stream's type prefix is irrelevant, and a type-dependent sidecar name breaks three real cases:
a stream archived/deleted while its snapshot remains (`IEventStoreSnapshot` does not require the
stream), `SaveSnapshotAsync` for an id without a stream, and any later format change. The `PASJ`
container is unchanged.

## Testing (guards, each red → green)

- **G1** `IdOnly` regression: a stream is `<id:D>.jsonl`; `ListAggregateIds` returns it;
  `LoadAsync` counts identical; **construction performs no directory scan** (assert the scan
  counter stays 0).
- **G2** `TypePrefix` enumeration: after appends the files are `{type}-{id}.jsonl`;
  `ListAggregateIds` returns **exactly** the ids (set equality — an empty result fails) and opens
  **zero** files. The same test proves the counter is alive with a positive control first:
  `IdOnly` + 3 streams → `FilesOpened` delta **== 3**, then `TypePrefix` → **== 0**.
- **G3** Both formats return identical `GetVersionAsync` / `OpenAsync(End − 3)` / `PeekFirstAsync`
  results for the same events; the corrupt-first-line id is returned only by `TypePrefix`
  (documented deviation).
- **G4** Snapshots under `TypePrefix`: save/load in a **new store instance** (not the writer),
  `PASJ` bytes unchanged, and a snapshot for an id **without** a stream works.
- **G5** Mixed directory: the configured format is used; a short foreign name (`x.txt`) is
  counted as skipped without throwing; the other is counted in
  `ForeignFormatFiles` (one Warning per scan); a **non-empty** other-format file for the same id
  makes `AppendAsync` throw naming both paths; a **zero-length** one is ignored.
- **G6** A `TypePrefix`-only directory opened with the default options → `ListAggregateIdsAsync`
  **throws** (whole-directory misconfiguration), not an empty list.
- **G8** A host that **never enumerates**: an `IdOnly` store with a `{type}-{id}` stream present →
  the first stream-creating `AppendAsync` performs the lazy pass and **throws** (whole-directory
  misconfiguration), not a silent second stream.
- **G7** A stream named with an old type name whose first event now deserializes as a new type:
  `IdOnly` enumerates it, `TypePrefix` does not (documented: renaming an event type requires a
  migration run).
- **Reverse-break (manual, review-time)**: replace the prefix slice with `LastIndexOf` → G2's id
  set becomes empty (red); replace the `TypePrefix` name scan with a peek → G2's `FilesOpened`
  becomes non-zero (red).
- **Performance (opt-in, not CI)**: fixed GUIDs, ~4,600 files, one warm-up + median of 5, the two
  formats measured over **two independent directories** (one directory per format — measuring both
  in one directory would make the second store see the first layout and throw); **plus** the new-id path (2,000 first appends)
  to measure the non-amortizable pass; no absolute threshold assertion; the measured numbers and
  the reproduction command go into `docs/release-notes/v2026.10.0.md`.

## Rev-5 delta (the older spec must not contradict this one)

- `2026-11-16-jsonl-event-store-design.md` §Intent: "PicoAgent adoption … needs a one-way file
  rename" is **no longer true** for a `TypePrefix` reader; the migration is a rename only for
  `IdOnly` adopters.
- §File layout: `<id:D>.jsonl` becomes "the default layout"; add the `TypePrefix` variant.
- §Failure policy, `ListAggregateIdsAsync` row: add the whole-directory misconfiguration throw and
  the five-bucket classification.
- §Testing: add G1–G8; the "corrupt first line → no id" case is now format-dependent (see G3).
- §Failure policy: add an `AppendAsync` row (same id, non-empty file in the other format →
  `InvalidOperationException` naming both paths; same id, several prefixes → `InvalidOperationException`
  listing the candidates).
- §Public surface: the internal diagnostic counters grow by `FilesOpened`, `ForeignFormatFiles`
  and `DirectoryScans` (test-only via `InternalsVisibleTo`; not in `api/`).

## Repo surface

- `api/PicoActor.EventStore.Jsonl.public.txt` changes (new `T:` for the enum, new `P:` on the
  options) — never pre-refresh; `release.ps1` refreshes at release time.
- `README.md` ×10: the JSONL section mentions both naming modes and the migration rule.
- `picohex-actor` skill: the option + the duplicate/whole-directory rules.
- Class-level XML docs: `<root>/<id:D>.jsonl` → "the default layout"; the `SkipFileName`
  warning text must not claim a stream is always `<guid>.jsonl`.
- `docs/release-notes/v2026.10.0.md`: x bump, the option, the cost model, the measured numbers.

## Compatibility / versioning

- `IdOnly` is the default: existing stores and tests are byte-identical.
- Additive public API (`FileNameFormat`, the property) → **x release** (`2026.10.0`).
- No wire change: the same lines and `PASJ` container; only names differ, and a half-migrated
  directory fails loudly by the rules above.

## Out of scope

Automatic migration/rename; reading both formats simultaneously (mixed directories are diagnosed,
not merged); multi-process writers.
