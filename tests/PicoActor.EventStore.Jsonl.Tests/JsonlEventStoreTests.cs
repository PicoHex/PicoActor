namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The append protocol: the order of the checks is the contract, so each test below pins one of them
/// — an empty batch is still checked, a rejected append changes no content, a torn tail is repaired by
/// the next append, and corruption is loud instead of silently dropped.
/// </summary>
public sealed class JsonlEventStoreTests
{
    [Test, Timeout(15000)]
    public async Task AppendLoad_RoundTrip_WithPolymorphicBase(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();

        // A base that carries ONLY [PicoDerivedType] must round-trip: the discriminator is written and
        // the value comes back as its concrete type (record equality would fail across types).
        var version = await store.AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2)]
        );

        await Assert.That(version).IsEqualTo(2UL);

        var loaded = await store.LoadAsync(id);

        await Assert.That(loaded.Count).IsEqualTo(2);
        await Assert.That(loaded[0]).IsEqualTo(new SSnapshotAdded(1));
        await Assert.That(loaded[1]).IsEqualTo(new SSnapshotAdded(2));
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(2UL);
    }

    [Test, Timeout(15000)]
    public async Task EmptyBatch_StaleVersion_Throws_NoContentChange(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        var path = StreamPath(temp.Root, id);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);

        // An empty batch is NOT an exemption from the external-modification check: the version is
        // still validated, and nothing on disk moves when it is wrong.
        await Assert
            .That(async () => await store.AppendAsync(id, 5, []))
            .Throws<ConcurrencyException>();

        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await Assert.That(after.SequenceEqual(before)).IsTrue();
        await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(2);
    }

    [Test, Timeout(15000)]
    public async Task EmptyBatch_FreshVersion_CreatesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        var version = await new JsonlEventStore<ConformanceEvent>(temp.Root).AppendAsync(
            Guid.NewGuid(),
            0,
            []
        );

        await Assert.That(version).IsEqualTo(0UL);
        await Assert.That(Directory.EnumerateFileSystemEntries(temp.Root).Any()).IsFalse();

        // The root itself is created on the first WRITE, never by a no-op append.
        var missing = Path.Combine(temp.Root, "fresh");
        await new JsonlEventStore<ConformanceEvent>(missing).AppendAsync(Guid.NewGuid(), 0, []);

        await Assert.That(Directory.Exists(missing)).IsFalse();
    }

    [Test, Timeout(15000)]
    public async Task NonTEventBase_Throws_ArgumentException(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        // Never a raw cast: the message names the offender and the stream's base type.
        var exception = await CaptureAsync(async () =>
            await store.AppendAsync(Guid.NewGuid(), 0, [new ForeignEvent(1)])
        );

        await Assert.That(exception).IsTypeOf<ArgumentException>();
        await Assert.That(exception.Message).Contains(nameof(ForeignEvent));
        await Assert.That(exception.Message).Contains(nameof(ConformanceEvent));
    }

    [Test, Timeout(15000)]
    public async Task TornTail_IsRepairedByNextAppend(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        await new JsonlEventStore<ConformanceEvent>(temp.Root).AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2)]
        );

        // A crash mid-write leaves an UNTERMINATED fragment. It is not an event: the count and the
        // loader ignore it, and the next append truncates it instead of appending after it.
        var path = StreamPath(temp.Root, id);
        await File.AppendAllTextAsync(path, "{\"$type\":\"ss-ad", cancellationToken);

        var restarted = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await Assert.That(await restarted.GetVersionAsync(id)).IsEqualTo(2UL);
        await Assert.That((await restarted.LoadAsync(id)).Count).IsEqualTo(2);

        var version = await restarted.AppendAsync(id, 2, [new SSnapshotAdded(3)]);

        await Assert.That(version).IsEqualTo(3UL);
        var loaded = await restarted.LoadAsync(id);
        await Assert.That(loaded.Count).IsEqualTo(3);
        await Assert.That(loaded[2]).IsEqualTo(new SSnapshotAdded(3));

        // The fragment is gone, not terminated: exactly three committed lines remain, ending on the
        // newline the store wrote. (Terminating the fragment instead would have left a fourth line.)
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        await Assert.That(bytes[^1]).IsEqualTo((byte)'\n');
        await Assert.That(bytes.Count(value => value == Newline)).IsEqualTo(3);
    }

    [Test, Timeout(15000)]
    public async Task CorruptTerminatedLine_Throws(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        var path = StreamPath(temp.Root, id);
        var lines = await File.ReadAllLinesAsync(path, cancellationToken);

        // Middle: a TERMINATED line that names no known event is corruption, not a torn write.
        await File.WriteAllLinesAsync(
            path,
            [lines[0], "{\"$type\":\"not-a-registered-event\"}", lines[1]],
            cancellationToken
        );
        await Assert.That(async () => await store.LoadAsync(id)).Throws<FormatException>();

        // Final: the same rule holds for the last line — only the UNTERMINATED one is forgiven.
        await File.WriteAllLinesAsync(
            path,
            [lines[0], lines[1], "not json at all"],
            cancellationToken
        );
        await Assert.That(async () => await store.LoadAsync(id)).Throws<FormatException>();

        // The cursor classifies lines by the same rule, so it throws too instead of skipping.
        await Assert
            .That(async () =>
            {
                await using var cursor = await store.OpenAsync(id, 1);
                while (await cursor.MoveNextAsync())
                {
                    // drain
                }
            })
            .Throws<FormatException>();
    }

    [Test, Timeout(15000)]
    public async Task PowerLossGarbage_IsCorruption_NotRepaired(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // FlushToDisk=false can leave TERMINATED garbage after a power loss. That is corruption: the
        // store reports it and does NOT repair it (an operator removes the line).
        var path = StreamPath(temp.Root, id);
        await File.AppendAllTextAsync(path, "~~~~ garbage ~~~~\n", cancellationToken);
        var before = await File.ReadAllBytesAsync(path, cancellationToken);

        await Assert.That(async () => await store.LoadAsync(id)).Throws<FormatException>();

        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await Assert.That(after.SequenceEqual(before)).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task ExternalLengthChange_Throws_Concurrency(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new SSnapshotAdded(2)]);

        // A write after an external modification could fork the history: it is refused
        // unconditionally, and only the cache is re-seeded.
        var path = StreamPath(temp.Root, id);
        await File.WriteAllTextAsync(path, string.Empty, cancellationToken);

        await Assert
            .That(async () => await store.AppendAsync(id, 2, [new SSnapshotAdded(3)]))
            .Throws<ConcurrencyException>();

        // The re-seeded cache makes the retry at the REAL version succeed.
        await Assert.That(await store.AppendAsync(id, 0, [new SSnapshotAdded(9)])).IsEqualTo(1UL);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(1UL);
    }

    [Test, Timeout(15000)]
    public async Task UnregisteredDerived_ThrowsWrappedFormatException(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<PartialEventBase>(temp.Root);

        // The registration lives on the BASE: an undeclared derived type is a FormatException on the
        // first write, wrapped so the message names the base and the one registration form.
        var exception = await CaptureAsync(async () =>
            await store.AppendAsync(Guid.NewGuid(), 0, [new UnregisteredPartialEvent(1)])
        );

        await Assert.That(exception).IsTypeOf<FormatException>();
        await Assert.That(exception.Message).Contains(nameof(PartialEventBase));
        await Assert.That(exception.Message).Contains("PicoDerivedType");
    }

    [Test, Timeout(15000)]
    public async Task UnregisteredBase_ThrowsWrappedInvalidOperationException(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<UnregisteredEventBase>(temp.Root);

        // No [PicoDerivedType] anywhere means no generated serializer for the base at all.
        var exception = await CaptureAsync(async () =>
            await store.AppendAsync(Guid.NewGuid(), 0, [new UnregisteredEventImpl(1)])
        );

        await Assert.That(exception).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception.Message).Contains(nameof(UnregisteredEventBase));
        await Assert.That(exception.Message).Contains("PicoDerivedType");
    }

    [Test, Timeout(15000)]
    public async Task BlankTail_AppendAfterRestart_Succeeds(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        await new JsonlEventStore<ConformanceEvent>(temp.Root).AppendAsync(
            id,
            0,
            [new SSnapshotAdded(1), new SSnapshotAdded(2), new SSnapshotAdded(3)]
        );

        // Trailing blank lines are TERMINATED, so the stream is not torn: after a restart the version is
        // still 3 and the next append lands at 4 (the blank lines are simply left in place).
        var path = StreamPath(temp.Root, id);
        await File.AppendAllTextAsync(
            path,
            "   " + (char)10 + (char)9 + (char)10, // spaces and a tab, each newline-terminated
            cancellationToken
        );

        var restarted = new JsonlEventStore<ConformanceEvent>(temp.Root);

        await Assert.That(await restarted.GetVersionAsync(id)).IsEqualTo(3UL);
        await Assert
            .That(await restarted.AppendAsync(id, 3, [new SSnapshotAdded(4)]))
            .IsEqualTo(4UL);
        await Assert.That((await restarted.LoadAsync(id)).Count).IsEqualTo(4);
    }

    /// <summary>The newline byte every committed line ends with.</summary>
    private const byte Newline = 10;

    private static string StreamPath(string root, Guid actorId) =>
        Path.Combine(root, actorId.ToString("D") + ".jsonl");

    /// <summary>Run an action that must throw and hand back WHAT it threw, for message assertions.</summary>
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
