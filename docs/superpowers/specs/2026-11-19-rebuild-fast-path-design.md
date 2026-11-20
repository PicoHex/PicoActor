# Rebuild fast path — snapshot offset + vectorized line counting

Status: draft for implementation (2026-11-19). Scope: `PicoActor.Abs` (one additive record member),
`PicoActor.EventStore.Jsonl` (scan + snapshot header + rebuild). **Correctness is the acceptance bar:
every fast path must be differentially verified against today's byte-by-byte semantics.**

## Why (measured on a production stream)

`D:/PicoAgent App/data/event-store/SessionStarted-01a15eae-…jsonl` = **47.2 MB / 31,752 events**, no snapshot.

```
rebuild today      = full byte scan 103.6–173.5 ms + replay 12.1 ms
  of which scan    = 455 MB/s (per-byte loop over `\n` + blank detection)
tail read if the snapshot carried its offset = 1.7 ms (4.7 MB)
SIMD newline count on the same file          = 21.0 ms (2.2 GB/s) — 8.2×
```

Two independent wins, one per cause:

| Cause | Fix | Effect |
|---|---|---|
| the store cannot address a version, so a rebuild must number every line | the snapshot records the byte offset it covers | rebuild = seek + read the tail (103.6 → 1.7 ms on the sample) |
| the scan itself is a per-byte loop | vectorize line counting, keeping the blank/torn-tail rules identical | 3–8× on every remaining scan (`GetVersionAsync`, enumeration, fallbacks) |

## Invariants (these are the contract; a fast path that breaks one is a bug)

- **I1 — same answer as today.** For any file and any `fromVersion` in `1..n+1`, `OpenAsync` must yield
  exactly the same events, the same `End`, the same torn-tail report and the same thrown exceptions as the
  current byte-by-byte implementation. Line semantics are unchanged: **a line counts iff it is terminated by
  `\n` and contains at least one byte outside `{ ' ', '\t', '\r' }`**; bytes after the last `\n` are a torn tail
  (reported, never counted).
- **I2 — an offset is advisory, and it is verified twice over.** A snapshot's `StreamOffset` is believed only
  when it is `0` (empty stream) or all of these hold:
  - it is a **line start** (`file[offset-1] == '
'`) **and** `offset <= fileLength`;
  - `streamLength` is coherent with it (`0 < offset <= streamLength <= fileLength`, both line starts), which is
    what makes a truncated / rolled back stream visible;
  - the **covered prefix still matches**: the FNV-1a witness the header records (I2b) equals the one recomputed
    over this file.
  Otherwise the rebuild ignores the offset, logs ONE warning and takes the scan path. No path may throw because
  of a bad offset. A header that contradicts itself (a position with no covered length, a covered length with no
  position, a position past the length it covers) is a SEPARATE warning: the calibration is dropped as unusable
  and said so, never silently zeroed.

  **The geometric tests alone are not sufficient, and saying so is the point of this revision.** "Is this byte a
  line start of this file" and "did the file shrink" prove only that the POSITION is addressable — they are blind
  to the covered prefix being replaced by another history with no fewer bytes. Such a file passes every
  geometric test, and an offset-only rebuild then derives its version from a drain of the wrong prefix: a
  silently different `Version`, no warning, no fallback. Reproduced against a real store before this revision: a
  5-event snapshot over a stream rewritten in place into 3 lines of the same length rebuilt `v5/555555` where a
  full replay of the very same file says `v3/333333`. The witness below is therefore a NECESSARY part of the fast
  path, not an optimisation of it.
- **I2b — a bounded content witness.** The header records `coveredPrefixWitness = FNV1a64(file[0 .. min(64,
  offset)]) ^ FNV1a64(file[offset - min(4096, offset) .. offset])`: the first 64 bytes and the 4096 bytes
  immediately BEFORE the position, XORed. Both windows are computed RELATIVE TO THE OFFSET, so appends after the
  snapshot leave them untouched and a fast-path snapshot keeps working as its tail grows (note
  `streamLength == streamOffset`: the snapshot is written after the append that reached its version). Writer and
  verifier run the SAME function — two positional reads, 4160 bytes, O(1), never a checksum of the covered
  history, which is exactly the read the fast path exists to avoid. `0` means "not measured": a header carrying
  it is not believed, and a writer that cannot measure the prefix attaches no calibration at all. A mismatch is
  ONE warning naming the covered prefix, then the scan path — never a throw, never a silent rebuild.
- **I3 — full replay stays the floor.** Missing / unreadable / schema-rejected / rolled-back / shrinking
  snapshots keep today's outcome: full replay on the same actor instance, never a second instance, never a
  failed rebuild. "Rolled back" is precise here: a stream truncated BELOW the covered length loses its snapshot
  (the recorded length exposes the shrink), and so does a stream whose covered prefix was rewritten (the witness
  exposes it); both then take the full replay, which is what the file now says.
- **I4 — one observation.** Whatever path is taken, the rebuilt `Version` and the reconciliation come from ONE
  read of the stream: the offset path derives the version as `snapshot.Version + linesRead`, and treats a
  torn tail at EOF exactly as the scan path does.
- **I5 — no format break.** Events keep the same bytes. The snapshot header grows from 16 to **44** bytes
  (4 magic + 8 version + 4 payloadLength + 4 reserved + 8 streamLength + 8 streamOffset + 8
  coveredPrefixWitness); a 16-byte (old) header must still decode — with every calibration field reported as `0`
  ("unknown"). The 36-byte layout of the pre-witness draft is deliberately NOT read: nothing ever shipped with
  it, and accepting it would mean a second live layout whose payload offset can drift.

- **I6 — the offset is captured, not computed.** `AppendAsync` records the write position it ended at, keyed by
  the version it produced; `SaveSnapshotAsync` attaches that recorded position and measures the witness of the
  bytes before it (I2b). `File.Length` must never be used for the POSITION (a concurrent append between the flush
  and the snapshot would make it describe a longer stream than the snapshot covers). When no recorded position
  matches the snapshot's version (fresh process, restored store, legacy snapshot) the header carries no
  calibration and the rebuild falls back to the scan.

## Design

### D1 — `ActorSnapshot` gains two optional members (additive, positional shape unchanged)

```csharp
public sealed record ActorSnapshot(ulong Version, byte[] Payload)
{
    /// <summary>Byte offset at which the FIRST event after <paramref name="Version"/> starts; 0 = unknown.</summary>
    public long StreamOffset { get; init; }

    /// <summary>Stream byte length this snapshot covers (== StreamOffset when the snapshot ends the file); 0 = unknown.</summary>
    public long StreamLength { get; init; }
}
```

`Deconstruct`/positional construction stay exactly as they are, so no existing caller changes.
The covered-prefix witness is deliberately NOT one of these members: it is a property of the FILE the store wrote,
and a caller that copies a snapshot's version and payload into a record of its own could not produce a truthful one
anyway. It lives in the header, it is read and verified inside `JsonlEventStore.LoadSnapshotAsync`, and it never
widens the public surface.

### D2 — the JSONL store records append positions, and witnesses them

`JsonlEventStore` keeps `ConcurrentDictionary<Guid, (ulong Version, long Offset)> _tailOffsets`, written by the
append path **after** the batch is flushed (the position it ends at), overwritten by the next append of that id.
`SaveSnapshotAsync` attaches `StreamOffset`/`StreamLength` from it when the version matches, else `0` — and, in
the same call, MEASURES the covered-prefix witness over the stream file (two positional reads, I2b) and writes it
into the header. A position whose prefix cannot be measured writes NO calibration at all (0/0/0), because every
later rebuild would have to refuse it anyway, one warning at a time, on a path nobody is watching.
Bounded like the other tables (one small struct per id that appended in this process; evicted with the aggregate
if the store ever grows that path — today it is unbounded, same as `_typeById`, and acceptable because it is one
entry per live aggregate).

### D3 — header 16 → 44 bytes, old headers still decode

```
"PASJ" | u64 version | u32 payloadLength | u32 reserved(0) | u64 streamLength | u64 streamOffset
       | u64 coveredPrefixWitness | payload
```
`TryDecode` returns `(version, payload, calibration)` where the calibration is `(streamLength, streamOffset,
coveredPrefixWitness, unusableReason)`; a 16-byte file decodes with every field `0` ("unknown") and no reason —
that is a legacy file, not a fault, so it raises no warning. `Encode` writes 44 bytes. A 16-byte reader (an older
store) must still read a 44-byte header: it validates `file.Length - HeaderBytes == payloadLength`, so the new
header breaks it — **therefore the reserved u32 exists**: old readers are only correct for 16-byte headers, and a
header written with `payloadLength` at the old position would be read as a payload of the wrong length.
Consequence, stated explicitly: **the snapshot file format is forward-only — an older PicoActor reading a newer
snapshot sees an invalid length, returns "no snapshot", and replays the stream in full.** That is the documented,
safe degradation (a snapshot is an accelerator), so no dual-layout is needed. The reason string is what keeps the
OTHER direction honest: a header that contradicts itself is dropped BY THE DECODER and reported BY THE STORE, so
"no calibration was ever recorded" and "this calibration is damaged" stay distinguishable to an operator.

### D4 — rebuild uses the offset when it verifies

`RebuildFromReaderAsync`: if `snapshot.StreamOffset > 0` and it passes I2's checks — geometry, covered length and
the recomputed witness — open a **seek cursor** at `StreamOffset` (`IEventLogReader.OpenAtOffsetAsync`, new
optional capability on the JSONL store: same cursor, no line numbering) and derive
`End = snapshot.Version + linesRead`. Otherwise the current `OpenAsync(v+1)` path. Both paths converge on
`ReplayEventsFrom(v, tail)`.

The witness is verified in `LoadSnapshotAsync`, i.e. where the calibration is read, and NOT only in the open that
uses it: the two are the same store's, and a caller that loads a snapshot and then decides for itself must see a
calibration that has already been checked or already been dropped. On the offset path the drain's count is what
`End` is derived from, so that path cannot self-check a rollback — its protection IS this verification, which is
why the verification has to be able to see a rewritten prefix and not only a shrunken file.

### D5 — vectorized line counting, same rules

`ScanStream` keeps its signature and semantics; the inner loop becomes:
- find `\n` with `ReadOnlySpan<byte>.IndexOf` (vectorized) — every hit is a line boundary;
- for each line, decide "non-blank" with a vectorized `IndexOfAnyExcept(' ', '\t', '\r')` over the line's bytes;
- keep the exact offset bookkeeping (`offset` = first byte of `fromVersion`) and the torn-tail rule.
Where the runtime proves fast enough, a single-pass mask implementation may replace it — the tests are the judge.

## Testing (the acceptance bar)

- **Differential (I1)**: a reference implementation keeps today's byte loop; a randomized corpus (blank lines,
  CRLF, trailing spaces, very long lines, non-ASCII UTF-8 that must never be decoded as text, torn tail, empty
  file, one line without `\n`) is compared positionally: same events, same `End`, same exceptions — for **every**
  `fromVersion` in `1..n+1`.
- **Offset honesty (I2)**: snapshot with a bogus offset (mid-line, beyond EOF, on a `\r`), snapshot whose file was
  truncated, snapshot from a different file → warning + full replay, never a wrong state, never a throw.
- **Covered prefix (I2b)**: a real store writes both the stream and the snapshot; the stream is then REWRITTEN
  in place into fewer events of the SAME total length, with the recorded position still a line start and the last
  byte still a terminator — every geometric test passes. The rebuild must refuse the offset with exactly one
  warning, report `StreamOffset`/`StreamLength` as 0, and reach the state a full replay of that file reaches —
  not the snapshot's own, which is the silent wrong answer this case exists to pin.
- **Witness does not over-refuse**: the ordinary append → snapshot → append → restart lifecycle must still take
  the seek (`OffsetOpens == 1`) and deserialize only the tail.
- **Half calibrations (I2)**: a header with a position but no covered length, a covered length but no position,
  or a position past the length it covers → calibration dropped with ONE warning saying it is unusable, never a
  silent zero.
- **Capture correctness (I6)**: append → snapshot → append → new store instance (in-memory map empty) → rebuild
  must equal a rebuild from a fresh full replay (same events, same version).
- **Differential state (I4)**: for the same stream, "rebuild via offset" and "rebuild via scan" produce
  identical actor state (compare `Version` and the snapshot payload of both instances).
- **Header compatibility (I5)**: 16-byte snapshot → decodes, every calibration field 0, rebuild scans, NO warning
  (a legacy file is not a fault); a 44-byte snapshot → the calibration is used; a truncated crop of either, or a
  36-byte header from the pre-witness draft, → "no snapshot".
- **Perf (opt-in, `[Test, Explicit]`)**: 31,752-event / 47 MB fixture generated in the test's temp root, report
  scan vs offset rebuild; numbers go into the release notes.

## Non-goals

- A sparse line index for arbitrary `fromVersion` (only worth it when tools read random versions; revisit then).
- Any change to event bytes, naming, or the store's public surfaces beyond D1's two init-only members.
