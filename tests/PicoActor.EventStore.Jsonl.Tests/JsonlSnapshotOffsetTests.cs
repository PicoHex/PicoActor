namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// THE SNAPSHOT'S BYTE OFFSET — the rebuild fast path (spec 2026-11-19, D1-D4 / I2-I6).
/// <para>
/// A snapshot may carry the byte position at which the FIRST event after its version starts, so a
/// rebuild can seek instead of numbering every line. The offset is ADVISORY (I2): it is believed only
/// when it points at a line start inside the stream file, it never makes a rebuild throw, and every
/// shape that cannot be verified degrades to today's path — a snapshot restored and its tail read by
/// the byte scan.
/// </para>
/// <para>
/// The offset is CAPTURED, never computed (I6): the append path records the position it ended at,
/// keyed by the version it produced, and the snapshot writer attaches it only when that version is the
/// one the snapshot covers. <c>File.Length</c> is deliberately useless here — an append between the
/// flush and the snapshot would make it describe a longer stream than the snapshot covers.
/// </para>
/// </summary>
public sealed class JsonlSnapshotOffsetTests
{
    // The container's field layout, spelled out: 4 magic, 8 version, 4 payloadLength, 4 reserved,
    // 8 streamLength, 8 streamOffset, 8 coveredPrefixWitness, then the payload. Sums to 44 — the spec's
    // prose said "36" before the witness was added, but the FIELD LIST is the wire format and is what
    // these tests pin.
    private const int Header = 44;
    private const int LegacyHeader = 16;
    private const int StreamLengthAt = 20;
    private const int StreamOffsetAt = 28;
    private const int CoveredPrefixWitnessAt = 36;

    // ── The header (I5 / D3) ────────────────────────────────────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Header_Layout_PutsEveryFieldWhereTheFormatSays(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        // A version whose every byte differs, and offsets that are NOT equal to each other, so a
        // swapped or omitted field cannot agree by accident.
        const ulong version = 0x0102030405060708UL;
        var bytes = JsonlSnapshotFile.Encode(
            version,
            [0xAA, 0xBB],
            new SnapshotCalibration(
                StreamLength: 0x1122334455667788L,
                StreamOffset: 0x0102030405060707L,
                CoveredPrefixWitness: 0x0F0E0D0C0B0A0908UL
            )
        );

        await Assert.That(bytes.Length).IsEqualTo(Header + 2);
        await Assert.That(Encoding.ASCII.GetString(bytes, 0, 4)).IsEqualTo("PASJ");

        // Little-endian, byte by byte: the format is host-independent, never BitConverter.
        await Assert.That(bytes[4]).IsEqualTo((byte)0x08);
        await Assert.That(bytes[11]).IsEqualTo((byte)0x01);
        await Assert.That(bytes[12]).IsEqualTo((byte)2); // payloadLength, low byte
        await Assert.That(bytes[13]).IsEqualTo((byte)0);

        // The reserved u32 between the length and the length-of-stream: zero, and load-bearing — it is
        // what makes an OLD 16-byte reader see a payload length that cannot match the bytes on disk.
        await Assert.That(bytes[16]).IsEqualTo((byte)0);
        await Assert.That(bytes[19]).IsEqualTo((byte)0);

        await Assert
            .That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(StreamLengthAt, 8)))
            .IsEqualTo(0x1122334455667788UL);
        await Assert
            .That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(StreamOffsetAt, 8)))
            .IsEqualTo(0x0102030405060707UL);

        // The covered-prefix WITNESS, last: the field that says WHICH history the two positions describe.
        // A positional field whose every byte differs, so an omitted or misplaced write cannot agree.
        await Assert
            .That(BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(CoveredPrefixWitnessAt, 8)))
            .IsEqualTo(0x0F0E0D0C0B0A0908UL);
        await Assert.That(bytes[Header]).IsEqualTo((byte)0xAA);

        // An OLD reader's check — <c>file.Length - 16 == payloadLength</c> — is FALSE for this file: the
        // documented, forward-only degradation (a newer snapshot reads as "no snapshot", full replay).
        await Assert.That(bytes.Length - LegacyHeader).IsNotEqualTo(2);
    }

    [Test, Timeout(15000)]
    public async Task Header_LegacySixteenBytes_StillDecodes_WithUnknownOffsets(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        // Exactly what an older store wrote: magic + version + payloadLength + payload, no offsets.
        var legacy = new byte[LegacyHeader + 3];
        Encoding.ASCII.GetBytes("PASJ").CopyTo(legacy, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(legacy.AsSpan(4, 8), 7UL);
        BinaryPrimitives.WriteUInt32LittleEndian(legacy.AsSpan(12, 4), 3);
        legacy[LegacyHeader] = 0xAA;
        legacy[LegacyHeader + 1] = 0xBB;
        legacy[LegacyHeader + 2] = 0xCC;

        var decoded = JsonlSnapshotFile.TryDecode(
            legacy,
            out var version,
            out var payload,
            out var calibration
        );

        await Assert.That(decoded).IsTrue();
        await Assert.That(version).IsEqualTo(7UL);
        await Assert.That(payload.SequenceEqual(new byte[] { 0xAA, 0xBB, 0xCC })).IsTrue();

        // "Unknown", not "empty": 0 is what makes the rebuild take the scan path, and NOT a fault — an
        // older store simply calibrated nothing, so nothing is owed a warning.
        await Assert.That(calibration.StreamLength).IsEqualTo(0L);
        await Assert.That(calibration.StreamOffset).IsEqualTo(0L);
        await Assert.That(calibration.CoveredPrefixWitness).IsEqualTo(0UL);
        await Assert.That(calibration.UnusableReason).IsNull();
    }

    [Test, Timeout(15000)]
    public async Task Header_TruncatedCropsOfEitherLayout_AreNoSnapshot(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        var current = JsonlSnapshotFile.Encode(3, [1, 2, 3], new SnapshotCalibration(40, 40, 7));
        var legacy = new byte[LegacyHeader + 3];
        Encoding.ASCII.GetBytes("PASJ").CopyTo(legacy, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(legacy.AsSpan(4, 8), 3UL);
        BinaryPrimitives.WriteUInt32LittleEndian(legacy.AsSpan(12, 4), 3);
        legacy[LegacyHeader] = 1;

        // A crop that stops inside the new header, and one that stops inside the old one.
        await Assert
            .That(JsonlSnapshotFile.TryDecode(current.AsSpan(0, Header - 1), out _, out _, out _))
            .IsFalse();
        await Assert
            .That(
                JsonlSnapshotFile.TryDecode(legacy.AsSpan(0, LegacyHeader - 1), out _, out _, out _)
            )
            .IsFalse();

        // A payloadLength that disagrees with the bytes on disk.
        var tampered = JsonlSnapshotFile.Encode(3, [1, 2, 3], SnapshotCalibration.Unknown);
        tampered[12] = 9;
        await Assert.That(JsonlSnapshotFile.TryDecode(tampered, out _, out _, out _)).IsFalse();

        // The THIRTY-SIX-byte layout this store wrote before the witness existed is NOT read: the format
        // is forward-only and unpublished, and a 36-byte file would otherwise have its payload start
        // eight bytes early — the exact silent misread <see cref="JsonlSnapshotFile.HeaderBytes"/> pins.
        var previous = new byte[36 + 3];
        Encoding.ASCII.GetBytes("PASJ").CopyTo(previous, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(previous.AsSpan(4, 8), 3UL);
        BinaryPrimitives.WriteUInt32LittleEndian(previous.AsSpan(12, 4), 3);
        BinaryPrimitives.WriteUInt64LittleEndian(previous.AsSpan(20, 8), 40UL);
        BinaryPrimitives.WriteUInt64LittleEndian(previous.AsSpan(28, 8), 40UL);
        previous[36] = 1;
        await Assert.That(JsonlSnapshotFile.TryDecode(previous, out _, out _, out _)).IsFalse();
    }

    // ── Capture (I6 / D2) ──────────────────────────────────────────────────────────────────────

    [Test, Timeout(30000)]
    public async Task Snapshot_CapturesTheAppendOffset_AndANewStoreReadsItBack(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        await store.AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2), new SSnapshotAdded(3)]
        );

        // The offset the append path ended at: the position just past the third line.
        var positionAfterThree = new FileInfo(StreamPath(temp.Root, id)).Length;

        await store.SaveSnapshotAsync(id, new ActorSnapshot(3, CounterPayload(6)));

        var written = await store.LoadSnapshotAsync(id);
        await Assert.That(written).IsNotNull();
        await Assert.That(written!.StreamOffset).IsEqualTo(positionAfterThree);
        await Assert.That(written.StreamLength).IsEqualTo(positionAfterThree);

        // Two more events land AFTER the snapshot was written.
        await store.AppendAsync(id, 3, [new SSnapshotAdded(4), new SSnapshotAdded(5)]);

        // A NEW store instance — an empty in-memory map — reads the SAME calibration back off the disk:
        // the pair is in the file, not in the process that wrote it.
        var restarted = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var reloaded = await restarted.LoadSnapshotAsync(id);

        await Assert.That(reloaded).IsNotNull();
        await Assert.That(reloaded!.StreamOffset).IsEqualTo(positionAfterThree);
        await Assert.That(reloaded.StreamLength).IsEqualTo(positionAfterThree);

        // And the rebuild pays for the TAIL only: two events deserialized out of five.
        var before = restarted.DeserializedEventCount;
        var system = NewSystem(restarted);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(5UL);
        await Assert.That(rebuilt.Value).IsEqualTo(15);
        await Assert.That(restarted.DeserializedEventCount - before).IsEqualTo(2L);

        // ... and it got there by SEEKING: the store's own witness says the offset was accepted once.
        await Assert.That(restarted.OffsetOpens).IsEqualTo(1L);
    }

    [Test, Timeout(30000)]
    public async Task Snapshot_CarriesNoOffset_WhenThisStoreDidNotProduceTheVersion(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        var writer = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await writer.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        // (a) A DIFFERENT instance wrote the events: its map is empty, so the version it is asked to
        //     snapshot was never appended HERE — an offset would be a guess, and a guess is 0.
        var stranger = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await stranger.SaveSnapshotAsync(id, new ActorSnapshot(2, CounterPayload(3)));
        var fromStranger = await stranger.LoadSnapshotAsync(id);
        await Assert.That(fromStranger!.StreamOffset).IsEqualTo(0L);
        await Assert.That(fromStranger.StreamLength).IsEqualTo(0L);

        // (b) The same instance appended PAST the snapshot's version: the recorded position describes
        //     version 4, not version 2, so it is not attached — and File.Length (longer again) is never
        //     consulted.
        await writer.AppendAsync(id, 2, [new SSnapshotAdded(3), new SSnapshotAdded(4)]);
        await writer.SaveSnapshotAsync(id, new ActorSnapshot(2, CounterPayload(3)));

        var staleVersion = await writer.LoadSnapshotAsync(id);
        await Assert.That(staleVersion!.Version).IsEqualTo(2UL);
        await Assert.That(staleVersion.StreamOffset).IsEqualTo(0L);
        await Assert.That(staleVersion.StreamLength).IsEqualTo(0L);
    }

    [Test, Timeout(30000)]
    public async Task Snapshot_OffsetIsTheCapturedPosition_NotTheCurrentFileLength(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);
        var positionAfterTwo = new FileInfo(StreamPath(temp.Root, id)).Length;
        await store.SaveSnapshotAsync(id, new ActorSnapshot(2, CounterPayload(3)));

        // The snapshot exists before these events were ever written: a File.Length taken NOW would
        // describe a stream the snapshot does not cover, and the rebuild would replay from the wrong
        // place. The CAPTURED position is what must survive.
        await store.AppendAsync(id, 2, [new SSnapshotAdded(3)]);
        var currentLength = new FileInfo(StreamPath(temp.Root, id)).Length;

        var snapshot = await store.LoadSnapshotAsync(id);

        await Assert.That(snapshot!.StreamOffset).IsEqualTo(positionAfterTwo);
        await Assert.That(snapshot.StreamOffset).IsNotEqualTo(currentLength);
        await Assert.That(currentLength > positionAfterTwo).IsTrue();
    }

    // ── The offset cursor (D4) ─────────────────────────────────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task OpenAtOffset_YieldsTheTail_WithDerivedVersions(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);
        var afterTwo = new FileInfo(StreamPath(temp.Root, id)).Length;
        await store.AppendAsync(id, 2, [new SSnapshotAdded(3), new SSnapshotAdded(4)]);

        var reader = (IEventLogOffsetReader)store;
        await using var cursor = await reader.OpenAtOffsetAsync(id, afterTwo, firstVersion: 3);

        await Assert.That(cursor).IsNotNull();

        // The line count is NOT measured by this open: the caller derives it from what it drains
        // (I4), so End is the explicit "not measured" sentinel rather than a number it never counted.
        await Assert.That(cursor!.End).IsEqualTo(ulong.MaxValue);

        var versions = new List<ulong>();
        var deltas = new List<int>();
        while (await cursor.MoveNextAsync())
        {
            versions.Add(cursor.CurrentVersion);
            deltas.Add(((SSnapshotAdded)cursor.Current).Delta);
        }

        await Assert.That(versions.SequenceEqual(new ulong[] { 3, 4 })).IsTrue();
        await Assert.That(deltas.SequenceEqual(new[] { 3, 4 })).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task OpenAtOffset_MidLine_IsRefused_WithOneWarningPerRefusal(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        var reader = (IEventLogOffsetReader)store;

        // Not a line start, and one byte past the end: no cursor, never a throw (I2).
        await Assert.That(await reader.OpenAtOffsetAsync(id, 5, firstVersion: 1)).IsNull();
        await Assert.That(await reader.OpenAtOffsetAsync(id, 100_000, firstVersion: 1)).IsNull();
        await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(2);
    }

    // ── Bad offsets at rebuild time (I2) ───────────────────────────────────────────────────────

    [Test, Timeout(30000)]
    public async Task BadOffset_MidLine_IsIgnored_AndTheRebuildIsCorrect(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        var logger = await RebuildBadOffsetAsync(
            temp.Root,
            id,
            events: 5,
            snapshotVersion: 2,
            snapshotValue: 3,
            streamLength: 100,
            streamOffset: 5,
            cancellationToken
        );
    }

    [Test, Timeout(30000)]
    public async Task BadOffset_BeyondTheEndOfTheStream_IsIgnored_AndTheRebuildIsCorrect(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        var logger = await RebuildBadOffsetAsync(
            temp.Root,
            id,
            events: 4,
            snapshotVersion: 2,
            snapshotValue: 3,
            streamLength: 100_000,
            streamOffset: 100_000,
            cancellationToken
        );
    }

    [Test, Timeout(30000)]
    public async Task BadOffset_OnACarriageReturn_IsIgnored_AndTheRebuildIsCorrect(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        // A blank, CRLF-terminated line: valid bytes for the line rule (blank => not an event), and the
        // byte BEFORE the offset is a '\r', not the '\n' a line start requires.
        var path = StreamPath(temp.Root, id);
        var before = new FileInfo(path).Length;
        await File.AppendAllTextAsync(path, " \r\n", cancellationToken);
        var offset = before + 2; // ' ' at before, '\r' at before+1, '\n' at before+2

        await WriteSnapshotAsync(
            temp.Root,
            id,
            snapshotVersion: 2,
            snapshotValue: 3,
            streamLength: offset,
            streamOffset: offset
        );

        // The snapshot itself is intact; the OFFSET is what was refused.
        var snapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(snapshot!.Version).IsEqualTo(2UL);
        await Assert.That(snapshot.StreamOffset).IsEqualTo(0L);
        await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(1);

        var rebuildStore = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var system = NewSystem(rebuildStore);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(2UL);
        await Assert.That(rebuilt.Value).IsEqualTo(3);
    }

    [Test, Timeout(30000)]
    public async Task BadOffset_TruncatedStream_FallsBackToTheFullReplay(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        await store.AppendAsync(
            id,
            0,
            [
                new SSnapshotAdded(1),
                new SSnapshotAdded(2),
                new SSnapshotAdded(3),
                new SSnapshotAdded(4),
            ]
        );

        var path = StreamPath(temp.Root, id);
        var fileLength = new FileInfo(path).Length;
        var lengthAfterTwoLines = LengthAfterLines(path, 2);
        await Assert.That(lengthAfterTwoLines > 0).IsTrue();
        await Assert.That(lengthAfterTwoLines < fileLength).IsTrue();

        // The snapshot covers four events and records the stream length it covered; the file is now the
        // first two lines only — a rollback/truncation. The offset still points at a line start of the
        // OLD file, so only the recorded LENGTH can expose the shrink.
        await WriteSnapshotAsync(
            temp.Root,
            id,
            snapshotVersion: 4,
            snapshotValue: 10,
            streamLength: fileLength,
            streamOffset: fileLength
        );
        File.WriteAllBytes(
            path,
            File.ReadAllBytes(path).AsSpan(0, (int)lengthAfterTwoLines).ToArray()
        );

        var loaded = await store.LoadSnapshotAsync(id);
        await Assert.That(loaded!.Version).IsEqualTo(4UL);
        await Assert.That(loaded.StreamOffset).IsEqualTo(0L);
        await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(1);

        var rebuildStore = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var before = rebuildStore.DeserializedEventCount;
        var system = NewSystem(rebuildStore);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();

        // Rollback (I3): the snapshot is discarded and the WHOLE remaining stream is replayed on the
        // same instance — never a second actor, never an exception.
        await Assert.That(rebuilt!.Version).IsEqualTo(2UL);
        await Assert.That(rebuilt.Value).IsEqualTo(3);
        await Assert.That(rebuildStore.DeserializedEventCount - before).IsEqualTo(2L);
    }

    [Test, Timeout(30000)]
    public async Task BadOffset_CopiedFromAnotherStream_IsIgnored_AndTheRebuildIsCorrect(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var other = Guid.NewGuid();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        // A SECOND stream whose lines are two bytes wide: its line starts (2, 4, 6) are positions a
        // snapshot of THAT stream would record, and they are mid-line in the first stream.
        var otherPath = StreamPath(temp.Root, other);
        await File.WriteAllTextAsync(otherPath, "1\n2\n3\n", cancellationToken);
        const long foreignLineStart = 2;

        await WriteSnapshotAsync(
            temp.Root,
            id,
            snapshotVersion: 2,
            snapshotValue: 3,
            streamLength: foreignLineStart,
            streamOffset: foreignLineStart
        );

        var snapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(snapshot!.StreamOffset).IsEqualTo(0L);
        await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(1);

        var rebuildStore = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var system = NewSystem(rebuildStore);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(2UL);
        await Assert.That(rebuilt.Value).IsEqualTo(3);
    }

    [Test, Timeout(30000)]
    public async Task BadOffset_CoveredPrefixRewrittenWithFewerLines_IsRefused_AndTheRebuildMatchesFullReplay(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );

        // THE REAL STORE writes BOTH the stream and the snapshot: the header, the offset and (after the
        // witness work) the prefix witness are the ones production writes. This test assembles neither.
        await store.AppendAsync(
            id,
            0,
            [
                new SSnapshotAdded(111_111),
                new SSnapshotAdded(111_111),
                new SSnapshotAdded(111_111),
                new SSnapshotAdded(111_111),
                new SSnapshotAdded(111_111),
            ]
        );
        var path = StreamPath(temp.Root, id);
        var originalLength = new FileInfo(path).Length;
        await store.SaveSnapshotAsync(id, new ActorSnapshot(5, CounterPayload(555_555)));

        var written = await store.LoadSnapshotAsync(id);
        await Assert.That(written!.StreamOffset).IsEqualTo(originalLength);
        await Assert.That(written.StreamLength).IsEqualTo(originalLength);
        await Assert.That(CountPrefixWitnessWarnings(logger)).IsEqualTo(0);

        // THE REWRITE — a different history of the SAME length. The first three lines survive verbatim;
        // everything after them becomes padding and ONE terminating newline, so the file's total length
        // is EXACTLY the recorded StreamOffset and its last byte is '\n'.
        //
        // Every GEOMETRIC test therefore passes: the position is inside the file, it is a line start, the
        // file did not shrink and its last byte ends a line. What changed is the PREFIX the snapshot was
        // taken over — the one thing an offset can never see for itself. Without a content witness this
        // rebuild seeks to EOF, derives End = 5 + 0 and restores v5/555555 out of a stream that says
        // v3/333333: a SILENT wrong state, with no warning and no fallback.
        var bytes = File.ReadAllBytes(path);
        var thirdLineEnd = (int)LengthAfterLines(path, 3);
        await Assert.That(thirdLineEnd > 0 && thirdLineEnd < originalLength).IsTrue();

        var rewritten = new byte[originalLength];
        Array.Copy(bytes, rewritten, thirdLineEnd);
        Array.Fill(rewritten, (byte)' ', thirdLineEnd, rewritten.Length - thirdLineEnd);
        rewritten[^1] = (byte)'\n';
        File.WriteAllBytes(path, rewritten);
        await Assert.That(new FileInfo(path).Length).IsEqualTo(originalLength);

        // I2: ONE warning naming the covered prefix, and the calibration is dropped wholesale — an
        // offset and a length that both still PASS every geometric check are still not believed.
        var reloaded = await store.LoadSnapshotAsync(id);
        await Assert.That(reloaded).IsNotNull();
        await Assert.That(reloaded!.Version).IsEqualTo(5UL);
        await Assert.That(reloaded.StreamOffset).IsEqualTo(0L);
        await Assert.That(reloaded.StreamLength).IsEqualTo(0L);
        await Assert.That(CountPrefixWitnessWarnings(logger)).IsEqualTo(1);

        // ... and the rebuild takes the scan path: the snapshot is newer than the stream it describes, so
        // it is discarded and the WHOLE remaining stream is replayed — version 3, value 333333.
        var rebuildLogger = new RecordingLogger();
        var rebuildStore = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = rebuildLogger }
        );
        var system = NewSystem(rebuildStore);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(3UL);
        await Assert.That(rebuilt.Value).IsEqualTo(333_333);

        // The state a full replay of the SAME file reaches — and emphatically NOT the snapshot's own
        // v5/555555, which is what an unwitnessed offset rebuild answered.
        await Assert.That(rebuilt.Version).IsNotEqualTo(5UL);
        await Assert.That(rebuilt.Value).IsNotEqualTo(555_555);
        await Assert.That(rebuildStore.OffsetOpens).IsEqualTo(0L);
        await Assert.That(CountPrefixWitnessWarnings(rebuildLogger)).IsEqualTo(1);
    }

    [Test, Timeout(30000)]
    public async Task GoodSnapshot_StillTakesTheOffsetPath_SoTheWitnessDoesNotRefuseIt(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );

        await store.AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2), new SSnapshotAdded(3)]
        );
        await store.SaveSnapshotAsync(id, new ActorSnapshot(3, CounterPayload(6)));
        await store.AppendAsync(id, 3, [new SSnapshotAdded(4), new SSnapshotAdded(5)]);

        // The ordinary lifecycle — append, snapshot, append — must keep the fast path: the witness is a
        // necessary part of the calibration, not a veto that turns every rebuild into a scan.
        var restarted = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var snapshot = await restarted.LoadSnapshotAsync(id);
        await Assert.That(snapshot!.StreamOffset > 0).IsTrue();
        await Assert.That(snapshot.StreamLength > 0).IsTrue();
        await Assert.That(CountPrefixWitnessWarnings(logger)).IsEqualTo(0);

        var before = restarted.DeserializedEventCount;
        var system = NewSystem(restarted);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(5UL);
        await Assert.That(rebuilt.Value).IsEqualTo(15);

        // SOUGHT, not counted: one accepted offset, and only the tail deserialized.
        await Assert.That(restarted.OffsetOpens).IsEqualTo(1L);
        await Assert.That(restarted.DeserializedEventCount - before).IsEqualTo(2L);
    }

    [Test, Timeout(30000)]
    public async Task HalfCalibrationInTheHeader_IsRefusedWithAWarning_NeverHalfBelieved(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);
        var length = new FileInfo(StreamPath(temp.Root, id)).Length;

        // Three headers that CONTRADICT THEMSELVES. Each is a calibration that describes nothing a
        // rebuild could act on, and each used to be zeroed in silence — an operator had no way to tell
        // "this store never recorded a position" from "this header is damaged".
        var cases = new (string Name, long StreamLength, long StreamOffset)[]
        {
            ("a position with no covered length", 0, length),
            ("a covered length with no position", length, 0),
            ("a position past the length it covers", length / 2, length),
        };

        foreach (var (name, streamLength, streamOffset) in cases)
        {
            var logger = new RecordingLogger();
            var reader = new JsonlEventStore<ConformanceEvent>(
                temp.Root,
                new JsonlEventStoreOptions { Logger = logger }
            );
            await WriteSnapshotAsync(
                temp.Root,
                id,
                snapshotVersion: 2,
                snapshotValue: 3,
                streamLength,
                streamOffset
            );

            var snapshot = await reader.LoadSnapshotAsync(id);

            // The snapshot is still a snapshot: its version and payload are the stream's business. Only
            // the calibration is dropped — and the ONE warning says so, naming the contradiction.
            await Assert.That(snapshot).IsNotNull();
            await Assert.That(snapshot!.Version).IsEqualTo(2UL);
            await Assert.That(snapshot.StreamOffset).IsEqualTo(0L);
            await Assert.That(snapshot.StreamLength).IsEqualTo(0L);
            await Assert.That(CountUnusableCalibrationWarnings(logger)).IsEqualTo(1);
            await Assert.That(logger.Contains("calibration is unusable")).IsTrue();
            await Assert.That(reader.OffsetOpens).IsEqualTo(0L);

            // No OTHER offset warning is owed: one damaged header is one line, not a cascade — and it is
            // not the "refused offset" line either, because an offset was never even put to the test.
            await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(0);
            _ = name;
        }
    }

    // ── Differential state (I4) ────────────────────────────────────────────────────────────────

    [Test, Timeout(30000)]
    public async Task OffsetRebuild_And_ScanRebuild_ProduceTheSameState(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        await store.AppendAsync(
            id,
            0,
            [
                new SSnapshotAdded(1),
                new SSnapshotAdded(2),
                new SSnapshotAdded(3),
                new SSnapshotAdded(4),
            ]
        );

        // ARM A — the snapshot is taken by the instance that appended, at the version it just wrote, so
        // it carries the captured offset; the two events that follow are its tail.
        await store.SaveSnapshotAsync(id, new ActorSnapshot(4, CounterPayload(10)));
        await store.AppendAsync(id, 4, [new SSnapshotAdded(5), new SSnapshotAdded(6)]);

        var offsetSnapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(offsetSnapshot!.StreamOffset > 0).IsTrue();

        var offsetBefore = store.DeserializedEventCount;
        var offsetSystem = NewSystem(store);
        var offsetActor = await offsetSystem.GetAsync<ProbeCounter>(id);
        var offsetDeserialized = store.DeserializedEventCount - offsetBefore;
        await offsetSystem.StopAsync(id);

        // ARM B — the SAME version and payload written by an instance that never appended: no captured
        // position, so the header carries 0 and the rebuild takes today's scan path.
        var scanner = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await scanner.SaveSnapshotAsync(id, new ActorSnapshot(4, CounterPayload(10)));
        var scanSnapshot = await scanner.LoadSnapshotAsync(id);
        await Assert.That(scanSnapshot!.StreamOffset).IsEqualTo(0L);

        var scanBefore = scanner.DeserializedEventCount;
        var scanSystem = NewSystem(scanner);
        var scanActor = await scanSystem.GetAsync<ProbeCounter>(id);
        var scanDeserialized = scanner.DeserializedEventCount - scanBefore;
        await scanSystem.StopAsync(id);

        await Assert.That(offsetActor).IsNotNull();
        await Assert.That(scanActor).IsNotNull();

        // I4: the two paths are two ways to the SAME state — version and payload, not just a value.
        await Assert.That(offsetActor!.Version).IsEqualTo(scanActor!.Version);
        await Assert.That(offsetActor.Version).IsEqualTo(6UL);
        await Assert.That(offsetActor.Value).IsEqualTo(scanActor.Value);
        await Assert
            .That(
                offsetActor
                    .SnapshotPayloadForAssert()!
                    .SequenceEqual(scanActor.SnapshotPayloadForAssert()!)
            )
            .IsTrue();

        // ... and the two arms really took DIFFERENT paths: A sought to the recorded position (one
        // accepted offset), B counted the stream's lines (none), and both reads still deserialized only
        // the snapshot's TAIL — that is what the cursor has always bought.
        await Assert.That(store.OffsetOpens).IsEqualTo(1L);
        await Assert.That(scanner.OffsetOpens).IsEqualTo(0L);
        await Assert.That(offsetDeserialized).IsEqualTo(2L);
        await Assert.That(scanDeserialized).IsEqualTo(2L);
    }

    [Test, Timeout(30000)]
    public async Task OffsetRebuild_ReadsOnlyTheTail_AndStillReportsATornTail(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);
        await store.SaveSnapshotAsync(id, new ActorSnapshot(2, CounterPayload(3)));

        // One complete event THROUGH THE STORE (so its line carries the tag this store writes),
        // then a fragment a crash left: bytes with no terminating newline.
        await store.AppendAsync(id, 2, [new SSnapshotAdded(3)]);
        await File.AppendAllTextAsync(
            StreamPath(temp.Root, id),
            "{\"$type\":\"SSnap",
            cancellationToken
        );

        var restarted = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var snapshot = await restarted.LoadSnapshotAsync(id);
        await Assert.That(snapshot!.StreamOffset > 0).IsTrue();

        var before = restarted.DeserializedEventCount;
        var system = NewSystem(restarted);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo(3UL);
        await Assert.That(rebuilt.Value).IsEqualTo(6);

        // The torn tail is reported and is NOT an event — the same semantics the scan path has.
        await Assert.That(restarted.DeserializedEventCount - before).IsEqualTo(1L);
        await Assert.That(restarted.TornTailLines).IsEqualTo(1L);
        await Assert.That(restarted.OffsetOpens).IsEqualTo(1L);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The rebuild over a stream whose snapshot carries an UNTRUSTWORTHY offset: the store must drop
    /// the offset (one warning), and the rebuild must reach the same state a correct rebuild would,
    /// with no exception anywhere (I2).
    /// </summary>
    private static async Task<RecordingLogger> RebuildBadOffsetAsync(
        string root,
        Guid id,
        int events,
        ulong snapshotVersion,
        int snapshotValue,
        long streamLength,
        long streamOffset,
        CancellationToken cancellationToken
    )
    {
        _ = cancellationToken;
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        await store.AppendAsync(
            id,
            0,
            Enumerable.Range(1, events).Select(i => (IDomainEvent)new SSnapshotAdded(i)).ToList()
        );

        await WriteSnapshotAsync(
            root,
            id,
            snapshotVersion,
            snapshotValue,
            streamLength,
            streamOffset
        );

        var snapshot = await store.LoadSnapshotAsync(id);

        // The store REFUSED the offset: the snapshot itself is intact (its version and payload are the
        // stream's business, not the offset's), and the rebuild takes the path it takes today.
        await Assert.That(snapshot).IsNotNull();
        await Assert.That(snapshot!.Version).IsEqualTo(snapshotVersion);
        await Assert.That(snapshot.StreamOffset).IsEqualTo(0L);
        await Assert.That(snapshot.StreamLength).IsEqualTo(0L);

        // ONE warning for ONE ignored offset (the rebuild loads the snapshot again, and that second load
        // owes its own — so the count is taken HERE, where the contract is).
        await Assert.That(CountOffsetWarnings(logger)).IsEqualTo(1);

        // ... and the fast path was never taken for this recovery.
        await Assert.That(store.OffsetOpens).IsEqualTo(0L);

        var system = NewSystem(store);
        var rebuilt = await system.GetAsync<ProbeCounter>(id);
        await system.StopAsync(id);

        await Assert.That(rebuilt).IsNotNull();

        // Every event is in the rebuilt state: the state is the full-stream state, not one derived
        // from an offset whose version mapping would have skipped or duplicated real events.
        await Assert.That(rebuilt!.Version).IsEqualTo((ulong)events);
        await Assert.That(rebuilt.Value).IsEqualTo(events * (events + 1) / 2);

        return logger;
    }

    /// <summary>
    /// Write a snapshot sidecar by hand, with the calibration the caller chose and NO covered-prefix
    /// witness (0 = unknown). That is deliberate: these tests are about the GEOMETRIC refusals, which sit
    /// in front of the witness in <c>VerifiableOffset</c>, so each one is still refused for its own
    /// reason — and a sidecar nobody measured a witness for is exactly what a hand-built header is.
    /// </summary>
    private static async Task WriteSnapshotAsync(
        string root,
        Guid id,
        ulong snapshotVersion,
        int snapshotValue,
        long streamLength,
        long streamOffset
    )
    {
        var directory = root + "-snapshots";
        Directory.CreateDirectory(directory);

        await File.WriteAllBytesAsync(
            Path.Combine(directory, id.ToString("D") + ".snap"),
            JsonlSnapshotFile.Encode(
                snapshotVersion,
                CounterPayload(snapshotValue),
                new SnapshotCalibration(streamLength, streamOffset, CoveredPrefixWitness: 0)
            )
        );
    }

    /// <summary>
    /// The probe actor's payload, byte for byte: <c>0x51</c> + the counter as a little-endian int. The
    /// payload a snapshot carries is the STATE, so two rebuilds that agree here agree about state.
    /// </summary>
    private static byte[] CounterPayload(int value)
    {
        var payload = new byte[5];
        payload[0] = 0x51;
        BitConverter.TryWriteBytes(payload.AsSpan(1), value);
        return payload;
    }

    private static ActorSystem NewSystem(IEventStore store)
    {
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<ProbeCounter>(
            command => new ProbeCounter((ProbeCreate)command),
            () => new ProbeCounter()
        );
        return system;
    }

    private static string StreamPath(string root, Guid id) =>
        Path.Combine(root, id.ToString("D") + ".jsonl");

    /// <summary>The byte position just past the <paramref name="lines"/>-th newline of a file.</summary>
    private static long LengthAfterLines(string path, int lines)
    {
        var bytes = File.ReadAllBytes(path);
        var seen = 0;
        for (var i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] == (byte)'\n' && ++seen == lines)
                return i + 1;
        }

        return 0;
    }

    /// <summary>The warning a refused offset owes: one line, naming the byte offset.</summary>
    private static int CountOffsetWarnings(RecordingLogger logger) =>
        logger.Messages.Count(message =>
            message.Contains("byte offset", StringComparison.OrdinalIgnoreCase)
        );

    /// <summary>
    /// The warning a MISMATCHED COVERED PREFIX owes: one line, naming the thing an offset cannot see. It
    /// is a separate counter because it is a separate judgement — the position is a line start of this
    /// file and the file did not shrink, and the calibration is still refused.
    /// </summary>
    private static int CountPrefixWitnessWarnings(RecordingLogger logger) =>
        logger.Messages.Count(message =>
            message.Contains("covered prefix", StringComparison.OrdinalIgnoreCase)
        );

    /// <summary>The warning a header that contradicts itself owes: one line, saying the calibration is unusable.</summary>
    private static int CountUnusableCalibrationWarnings(RecordingLogger logger) =>
        logger.Messages.Count(message =>
            message.Contains("calibration is unusable", StringComparison.OrdinalIgnoreCase)
        );
}
