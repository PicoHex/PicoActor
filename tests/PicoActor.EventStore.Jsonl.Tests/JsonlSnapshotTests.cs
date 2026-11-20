namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The snapshot sidecar: <c>&lt;root&gt;-snapshots/&lt;id&gt;.snap</c>, <c>PASJ</c> + little-endian header +
/// opaque payload. A snapshot is an ACCELERATOR, so the store never validates it against the stream —
/// the runtime's rollback guard is the one that discards a version the stream does not have.
/// </summary>
public sealed class JsonlSnapshotTests
{
    private const string Magic = "PASJ";

    [Test, Timeout(15000)]
    public async Task Snapshot_RoundTrip(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        var payload = new byte[] { 1, 2, 3, 4, 5 };

        await store.SaveSnapshotAsync(id, new ActorSnapshot(7, payload));
        var loaded = await store.LoadSnapshotAsync(id);

        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Version).IsEqualTo(7UL);
        await Assert.That(loaded.Payload.SequenceEqual(payload)).IsTrue();

        // Last write wins: a snapshot supersedes the previous one, it is not history.
        await store.SaveSnapshotAsync(id, new ActorSnapshot(9, [9]));
        var replaced = await store.LoadSnapshotAsync(id);

        await Assert.That(replaced!.Version).IsEqualTo(9UL);
        await Assert.That(replaced.Payload.SequenceEqual(new byte[] { 9 })).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_WritesToASiblingDirectory(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();

        await store.SaveSnapshotAsync(id, new ActorSnapshot(1, [1, 2, 3]));

        // The snapshot root is a sibling with a trailing-separator-normalized name, and the enumeration
        // never sees it: it walks the stream directory only.
        var snapshotDir = temp.Root + "-snapshots";
        await Assert.That(Directory.Exists(snapshotDir)).IsTrue();
        await Assert
            .That(File.Exists(Path.Combine(snapshotDir, id.ToString("D") + ".snap")))
            .IsTrue();

        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);
        await Assert.That(await store.ListAggregateIdsAsync(nameof(SSnapshotAdded))).Contains(id);
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_RootWithTrailingSeparator_NamesTheSameFile(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var plain = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var slashed = new JsonlEventStore<ConformanceEvent>(temp.Root + "/");
        var id = Guid.NewGuid();

        await plain.SaveSnapshotAsync(id, new ActorSnapshot(2, [2]));
        await slashed.SaveSnapshotAsync(id, new ActorSnapshot(4, [4]));

        // The trailing separator is normalized BEFORE the -snapshots sibling is derived, so both
        // spellings name ONE sidecar: the second write REPLACED the first instead of creating a
        // "data/-snapshots" directory, and a read through either store sees the same bytes.
        var snapshotDir = temp.Root + "-snapshots";
        await Assert.That(Directory.Exists(snapshotDir)).IsTrue();
        await Assert.That(Directory.Exists(temp.Root + "/-snapshots")).IsFalse();
        await Assert.That(Directory.EnumerateFiles(snapshotDir, "*.snap").Count()).IsEqualTo(1);
        await Assert.That((await plain.LoadSnapshotAsync(id))!.Version).IsEqualTo(4UL);
        await Assert.That((await slashed.LoadSnapshotAsync(id))!.Version).IsEqualTo(4UL);
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_ReadReturnsFreshArray(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        await store.SaveSnapshotAsync(id, new ActorSnapshot(3, [1, 2, 3]));

        var first = await store.LoadSnapshotAsync(id);
        var second = await store.LoadSnapshotAsync(id);

        // The contract permits handing out the stored array; this store copies, so two reads share no
        // buffer and a caller never needs ownership rules.
        await Assert.That(ReferenceEquals(first!.Payload, second!.Payload)).IsFalse();
        await Assert.That(first.Payload.SequenceEqual(second.Payload)).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_VersionAboveCount_ReturnedUnchanged(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // The store does not police the snapshot version: returning it unchanged is what lets the
        // runtime's rollback guard see the divergence and fall back to a full replay.
        await store.SaveSnapshotAsync(id, new ActorSnapshot(42, [7, 7, 7]));
        var loaded = await store.LoadSnapshotAsync(id);

        await Assert.That(loaded!.Version).IsEqualTo(42UL);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(1UL);
    }

    [Test, Timeout(15000)]
    public async Task SnapshotHeader_IsLittleEndian(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();

        // A version whose every byte differs: only an explicit little-endian write produces this order
        // (BitConverter/NativeEndian would come out reversed on a big-endian host and silently agree on
        // this one — the assertion is on the BYTES, which is what the format promises).
        const ulong version = 0x0102030405060708UL;
        await store.SaveSnapshotAsync(id, new ActorSnapshot(version, [0xAA, 0xBB, 0xCC]));

        var bytes = await File.ReadAllBytesAsync(
            Path.Combine(temp.Root + "-snapshots", id.ToString("D") + ".snap"),
            cancellationToken
        );

        // 44-byte header + the 3 payload bytes (spec 2026-11-19, D3: the header gained the two stream
        // positions and the covered-prefix witness). The 44 is a LITERAL, not JsonlSnapshotFile.HeaderBytes:
        // asserting the constant against itself would make this test agree with whatever the format became,
        // which is the one thing it exists to catch.
        await Assert.That(bytes.Length).IsEqualTo(44 + 3);
        await Assert.That(Encoding.ASCII.GetString(bytes, 0, 4)).IsEqualTo(Magic);
        await Assert.That(bytes[4]).IsEqualTo((byte)0x08);
        await Assert.That(bytes[5]).IsEqualTo((byte)0x07);
        await Assert.That(bytes[11]).IsEqualTo((byte)0x01);

        // bytes 12..16: the payload length, little-endian u32 — THE SAME POSITION the old layout used,
        // which is what makes an old reader's "file.Length - 16 == payloadLength" check fail loudly
        // rather than read a payload of the wrong length.
        await Assert.That(bytes[12]).IsEqualTo((byte)3);
        await Assert.That(bytes[13]).IsEqualTo((byte)0);

        // No stream appended for this id, so the store knows no position and witnessed no prefix: 0 =
        // "unknown" in all three calibration fields.
        await Assert.That(bytes[16]).IsEqualTo((byte)0); // reserved, 4 bytes
        await Assert.That(bytes[19]).IsEqualTo((byte)0);
        await Assert.That(bytes[20]).IsEqualTo((byte)0); // stream length, 8 bytes
        await Assert.That(bytes[27]).IsEqualTo((byte)0);
        await Assert.That(bytes[28]).IsEqualTo((byte)0); // stream offset, 8 bytes
        await Assert.That(bytes[35]).IsEqualTo((byte)0);
        await Assert.That(bytes[36]).IsEqualTo((byte)0); // covered-prefix witness, 8 bytes
        await Assert.That(bytes[43]).IsEqualTo((byte)0);

        await Assert.That(bytes[44]).IsEqualTo((byte)0xAA);
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_InvalidMagicOrTruncated_ReturnsNull(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var snapshotDir = temp.Root + "-snapshots";
        Directory.CreateDirectory(snapshotDir);

        // Missing.
        await Assert.That(await store.LoadSnapshotAsync(Guid.NewGuid())).IsNull();

        // Bad magic.
        var badMagic = Guid.NewGuid();
        await WriteSnapshotAsync(snapshotDir, badMagic, BuildSnapshot("XXXX", 1, [1, 2]));

        // Truncated header (fewer than 16 bytes).
        var truncated = Guid.NewGuid();
        await WriteSnapshotAsync(snapshotDir, truncated, [0x50, 0x41, 0x53]);

        // Version 0 covers no events, so it describes nothing a recovery could act on.
        var zeroVersion = Guid.NewGuid();
        await WriteSnapshotAsync(snapshotDir, zeroVersion, BuildSnapshot(Magic, 0, [1]));

        // A payloadLength that does not match the bytes on disk is a torn write, not a snapshot.
        var lengthMismatch = Guid.NewGuid();
        var mismatched = BuildSnapshot(Magic, 5, [1, 2, 3]);
        mismatched[12] = 99;
        await WriteSnapshotAsync(snapshotDir, lengthMismatch, mismatched);

        foreach (var id in new[] { badMagic, truncated, zeroVersion, lengthMismatch })
        {
            await Assert.That(await store.LoadSnapshotAsync(id)).IsNull();
        }
    }

    [Test, Timeout(15000)]
    public async Task Snapshot_AtomicReplace_RemovesTempOnFailure(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        var snapshotDir = temp.Root + "-snapshots";
        Directory.CreateDirectory(snapshotDir);

        // The rename target is a DIRECTORY, so the atomic replace fails after the temp file was written
        // and flushed. The failure is reported, and the leftover temp file — which would otherwise be a
        // second file per failed write, forever — is cleaned up.
        Directory.CreateDirectory(Path.Combine(snapshotDir, id.ToString("D") + ".snap"));

        var exception = await CaptureAsync(async () =>
            await store.SaveSnapshotAsync(id, new ActorSnapshot(1, [1, 2, 3]))
        );

        await Assert.That(exception is IOException or UnauthorizedAccessException).IsTrue();
        await Assert.That(Directory.EnumerateFiles(snapshotDir, "*.tmp").Any()).IsFalse();
    }

    [Test, Timeout(15000)]
    public async Task SnapshotRebuild_DeserializesOnlyTheTail(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        const int total = 6;
        await store.AppendAsync(
            id,
            0,
            Enumerable.Range(1, total).Select(i => (IDomainEvent)new SSnapshotAdded(i)).ToList()
        );

        const ulong snapshotVersion = 4;
        await store.SaveSnapshotAsync(id, new ActorSnapshot(snapshotVersion, [1]));
        var before = store.DeserializedEventCount;

        // The rebuild opens at version + 1: End comes from the byte scan, and only the TAIL is
        // deserialized — the head is read once as bytes and never handed to the serializer.
        var seen = 0;
        await using (var cursor = await store.OpenAsync(id, snapshotVersion + 1))
        {
            await Assert.That(cursor.End).IsEqualTo((ulong)total);
            while (await cursor.MoveNextAsync())
                seen++;
        }

        await Assert.That(seen).IsEqualTo(total - (int)snapshotVersion);
        await Assert
            .That(store.DeserializedEventCount - before)
            .IsEqualTo(total - (long)snapshotVersion);
    }

    [Test, Timeout(15000)]
    public async Task OpenPastVertex_DeserializesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);
        var before = store.DeserializedEventCount;

        // A snapshot NEWER than the stream is the rollback path: the cursor is empty and End is the
        // real length, so the rebuild deserializes nothing at all.
        await using var cursor = await store.OpenAsync(id, 3);

        await Assert.That(cursor.End).IsEqualTo(2UL);
        await Assert.That(await cursor.MoveNextAsync()).IsFalse();
        await Assert.That(store.DeserializedEventCount).IsEqualTo(before);
    }

    private static byte[] BuildSnapshot(string magic, ulong version, byte[] payload)
    {
        var bytes = new byte[16 + payload.Length];
        Encoding.ASCII.GetBytes(magic).CopyTo(bytes, 0);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(4, 8), version);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(12, 4), (uint)payload.Length);
        payload.CopyTo(bytes, 16);
        return bytes;
    }

    private static Task WriteSnapshotAsync(string directory, Guid id, byte[] bytes) =>
        File.WriteAllBytesAsync(Path.Combine(directory, id.ToString("D") + ".snap"), bytes);

    /// <summary>Run an action that must throw and hand back WHAT it threw, for shape assertions.</summary>
    private static async Task<Exception> CaptureAsync(Func<ValueTask> action)
    {
        try
        {
            await action();
        }
        catch (Exception exception)
        {
            return exception;
        }

        throw new InvalidOperationException("The operation was expected to throw and did not.");
    }
}
