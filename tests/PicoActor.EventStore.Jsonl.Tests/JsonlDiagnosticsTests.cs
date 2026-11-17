namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The operator-facing surface: the gate table is BOUNDED (a long-lived host touches thousands of
/// aggregates), and the three counters plus <see cref="JsonlEventStoreOptions.Logger"/> are how a
/// silent corruption policy is made observable.
/// </summary>
public sealed class JsonlDiagnosticsTests
{
    [Test, Timeout(30000)]
    public async Task LockEntryCount_StaysBounded(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { MaxAggregateLocks = 8 }
        );

        // A daemon appends to thousands of distinct aggregates over its life. One live semaphore per
        // aggregate forever is a leak; the bound is what keeps the table flat.
        for (var i = 0; i < 40; i++)
            await store.AppendAsync(Guid.NewGuid(), 0, [new SSnapshotAdded(i)]);

        await Assert.That(store.LockEntryCount).IsLessThanOrEqualTo(8);
        await Assert.That(store.LockEntryCount).IsGreaterThan(0);
    }

    [Test, Timeout(15000)]
    public async Task Ctor_InvalidArgs_Throw(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // A store with no root cannot name a stream file.
        await Assert
            .That(() => new JsonlEventStore<ConformanceEvent>(null!))
            .Throws<ArgumentException>();
        await Assert
            .That(() => new JsonlEventStore<ConformanceEvent>("   "))
            .Throws<ArgumentException>();

        // A bound of zero would evict the entry an append is holding. The parameter NAME must be the
        // tunable that is actually wrong: "options" points the host at a record it passed as a whole,
        // which is not something it can fix by reading the message.
        var outOfRange = await Assert
            .That(() =>
                new JsonlEventStore<ConformanceEvent>(
                    temp.Root,
                    new JsonlEventStoreOptions { MaxAggregateLocks = 0 }
                )
            )
            .Throws<ArgumentOutOfRangeException>();
        await Assert
            .That(outOfRange!.ParamName)
            .IsEqualTo(nameof(JsonlEventStoreOptions.MaxAggregateLocks));
    }

    [Test, Timeout(15000)]
    public async Task TornTail_ReportsWarningAndCounter(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // One crash-torn fragment: ignored by the count, reported to the logger, and counted — a policy
        // that drops bytes must never be silent.
        await File.AppendAllTextAsync(
            Path.Combine(temp.Root, id.ToString("D") + ".jsonl"),
            "{\"$type\":\"torn",
            cancellationToken
        );

        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(1UL);
        await Assert.That(store.TornTailLines).IsEqualTo(1);
        await Assert.That(logger.Contains("unterminated")).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task TornTail_RepairedByCachedAppend_IsStillReported(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        var path = Path.Combine(temp.Root, id.ToString("D") + ".jsonl");
        await File.AppendAllTextAsync(path, "{\"$type\":\"torn", cancellationToken);

        // A stale-version append is refused AFTER the length witness forced a rescan, which leaves the
        // cache SEEDED at the torn length. The tail is therefore behind a cache hit from now on, not
        // behind a rescan.
        var refused = false;
        try
        {
            await store.AppendAsync(id, 9, [new SSnapshotAdded(2)]);
        }
        catch (ConcurrencyException)
        {
            refused = true;
        }

        await Assert.That(refused).IsTrue();
        await Assert.That(store.TornTailLines).IsEqualTo(1);

        // The next append takes the truncate branch with an already-seeded cache. A policy that drops
        // bytes must not be silent about THIS observation either: the warning and the counter are the
        // only evidence an operator has that a fragment was removed here.
        await Assert.That(await store.AppendAsync(id, 1, [new SSnapshotAdded(2)])).IsEqualTo(2UL);
        await Assert.That(store.TornTailLines).IsEqualTo(2);
        await Assert
            .That(
                logger.Messages.Count(message =>
                    message.Contains("unterminated", StringComparison.OrdinalIgnoreCase)
                )
            )
            .IsEqualTo(2);

        // The fragment never became an event, and the truncated stream is intact.
        var loaded = await store.LoadAsync(id);
        await Assert.That(loaded.Count).IsEqualTo(2);
        await Assert.That(loaded[1]).IsEqualTo(new SSnapshotAdded(2));
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(2UL);
    }

    [Test, Timeout(15000)]
    public async Task SkippedFileNames_ForForeignFiles(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(1)]);

        // A store directory is a shared directory: a README, a lease or an old naming scheme is skipped
        // (and counted), never enumerated as a stream and never fatal.
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, "README.md"),
            "not a stream",
            cancellationToken
        );
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, "not-a-guid.jsonl"),
            "{}",
            cancellationToken
        );

        var ids = await store.ListAggregateIdsAsync(nameof(SSnapshotAdded));

        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(id);
        await Assert.That(store.SkippedFileNames).IsEqualTo(2);
        await Assert.That(logger.Contains("README.md")).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task CorruptFirstLine_YieldsNoId_NoThrow(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();
        var path = Path.Combine(temp.Root, id.ToString("D") + ".jsonl");

        // A corrupt FIRST line makes the aggregate unclassifiable — which is an answer ("no id"), not a
        // boot failure, and not a skipped FILE either: the name is a perfectly good stream name.
        await File.WriteAllTextAsync(path, "~~~~ not an event ~~~\n", cancellationToken);

        await Assert.That(await store.PeekFirstAsync(id)).IsNull();
        await Assert.That(await store.ListAggregateIdsAsync(nameof(SSnapshotAdded))).IsEmpty();
        await Assert.That(store.SkippedFileNames).IsEqualTo(0);
    }
}
