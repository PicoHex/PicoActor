using PicoActor.Abs;

namespace PicoActor.TestSupport;

/// <summary>
/// Conformance contract every <see cref="IEventLogReader"/>/<see cref="IEventCursor"/>
/// implementation must satisfy. It is written against the two ABS interfaces only, so a store
/// (in-memory today, file later) proves its cursor semantics by inheriting this class instead of
/// re-deriving them from the docs.
/// <para>
/// The three seeded events (versions 1..3) are <see cref="SSnapshotAdded"/> records, whose value
/// equality lets an assertion compare a yielded event with the one the store holds.
/// </para>
/// </summary>
public abstract class EventLogReaderConformanceBase
{
    private const int Seeded = 3;

    /// <summary>
    /// The store under test paired with the same instance viewed as the read port: conformance
    /// is a property of the pair (the reader must agree with the store's own view of the stream).
    /// </summary>
    protected abstract (IEventStore Store, IEventLogReader Reader) Create();

    /// <summary>
    /// Append blank lines to an existing stream WITHOUT appending an event, when the store's medium
    /// has lines at all (a file). The default does nothing: a store whose streams are lists cannot
    /// hold a blank line, and the blank-line cases then assert only that the stream is intact.
    /// <para>
    /// The three shapes every reader must ignore are trailing ASCII spaces, a lone tab and a bare
    /// carriage return — all newline-TERMINATED, so the file is not torn, only wasteful.
    /// </para>
    /// </summary>
    protected virtual Task InjectBlankLinesAsync(Guid actorId) => Task.CompletedTask;

    /// <summary>Append <paramref name="count"/> events — versions 1..count — to a fresh stream.</summary>
    private static async Task<Guid> SeedAsync(IEventStore store, int count)
    {
        var id = Guid.NewGuid();
        var events = Enumerable
            .Range(1, count)
            .Select(i => (IDomainEvent)new SSnapshotAdded(i))
            .ToList();
        await store.AppendAsync(id, 0, events);
        return id;
    }

    [Test, Timeout(15000)]
    public async Task GetVersion_IsZero_ForMissingStream(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (_, reader) = Create();

        await Assert.That(await reader.GetVersionAsync(Guid.NewGuid())).IsEqualTo(0UL);
    }

    [Test, Timeout(15000)]
    public async Task GetVersion_MatchesLoadAsyncCount(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        var loaded = await store.LoadAsync(id);
        await Assert.That(await reader.GetVersionAsync(id)).IsEqualTo((ulong)loaded.Count);
    }

    [Test, Timeout(15000)]
    public async Task OpenAtOne_OnEmptyStream_EndsImmediately(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (_, reader) = Create();

        await using var cursor = await reader.OpenAsync(Guid.NewGuid(), 1);

        await Assert.That(cursor.End).IsEqualTo(0UL);
        await Assert.That(await cursor.MoveNextAsync()).IsFalse();
    }

    [Test, Timeout(15000)]
    public async Task OpenAtOne_YieldsAll_WithAscendingVersions(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);
        var expected = await store.LoadAsync(id);

        await using var cursor = await reader.OpenAsync(id, 1);

        await Assert.That(cursor.End).IsEqualTo(3UL);

        var versions = new List<ulong>();
        while (await cursor.MoveNextAsync())
        {
            versions.Add(cursor.CurrentVersion);
            await Assert.That(cursor.Current).IsEqualTo(expected[versions.Count - 1]);
        }

        await Assert.That(versions.Count).IsEqualTo(Seeded);
        await Assert.That(versions.SequenceEqual([1UL, 2UL, 3UL])).IsTrue();
        // Past the end the cursor stays exhausted.
        await Assert.That(await cursor.MoveNextAsync()).IsFalse();
    }

    [Test, Timeout(15000)]
    public async Task OpenAtV_YieldsVToEnd_WithEndUnchanged(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        await using var cursor = await reader.OpenAsync(id, 2);

        // End is the stream length at open, not the number of events this cursor yields.
        await Assert.That(cursor.End).IsEqualTo(3UL);

        var versions = new List<ulong>();
        while (await cursor.MoveNextAsync())
            versions.Add(cursor.CurrentVersion);

        await Assert.That(versions.SequenceEqual([2UL, 3UL])).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task OpenAtEndPlusOne_YieldsNothing_ButEndIsRealLength(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        // Beyond the end is LEGAL (the rollback path opens at snapshot.Version + 1): the cursor
        // is empty, but End still reports the actual length.
        await using var cursor = await reader.OpenAsync(id, 4);

        await Assert.That(cursor.End).IsEqualTo(3UL);
        await Assert.That(await cursor.MoveNextAsync()).IsFalse();
    }

    [Test, Timeout(15000)]
    public async Task OpenAtZero_Throws_ArgumentOutOfRange(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        await Assert
            .That(async () => await reader.OpenAsync(id, 0))
            .Throws<ArgumentOutOfRangeException>();
    }

    [Test, Timeout(15000)]
    public async Task Current_Throws_BeforeFirstMove_And_AfterEnd(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        await using var cursor = await reader.OpenAsync(id, 1);

        // Before the first successful MoveNextAsync there is no Current.
        await Assert.That(() => _ = cursor.Current).Throws<InvalidOperationException>();
        await Assert.That(() => _ = cursor.CurrentVersion).Throws<InvalidOperationException>();

        while (await cursor.MoveNextAsync()) { }

        // After the end the last event is no longer Current.
        await Assert.That(() => _ = cursor.Current).Throws<InvalidOperationException>();
        await Assert.That(() => _ = cursor.CurrentVersion).Throws<InvalidOperationException>();
    }

    [Test, Timeout(15000)]
    public async Task DisposeAsync_IsIdempotent(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        var cursor = await reader.OpenAsync(id, 1);
        await Assert.That(await cursor.MoveNextAsync()).IsTrue();
        var read = cursor.Current;
        var version = cursor.CurrentVersion;

        await cursor.DisposeAsync();
        // A second dispose is a no-op, not an exception.
        await cursor.DisposeAsync();

        // Disposal does not rewrite history: what was already read stays readable.
        await Assert.That(cursor.Current).IsEqualTo(read);
        await Assert.That(cursor.CurrentVersion).IsEqualTo(version);
    }

    [Test, Timeout(15000)]
    public async Task OpenAsync_HonorsCancellation(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        // Open is the FIRST read of a rebuild: a cancelled caller must not be handed a cursor
        // (nor leave one holding the store's per-actor gate).
        await Assert
            .That(async () => await reader.OpenAsync(id, 1, cancelled.Token))
            .Throws<OperationCanceledException>();
    }

    [Test, Timeout(15000)]
    public async Task PeekFirst_ReturnsFirstEvent(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);
        var expected = await store.LoadAsync(id);

        // Recovery enumeration reads the FIRST event of a stream: the reader must answer that
        // without materializing the stream, and agree with the store's own first element.
        var first = await reader.PeekFirstAsync(id);

        await Assert.That(first).IsNotNull();
        await Assert.That(first).IsEqualTo(expected[0]);
        await Assert.That(await reader.PeekFirstAsync(Guid.NewGuid())).IsNull();
    }

    [Test, Timeout(15000)]
    public async Task MoveNext_HonorsCancellation(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        await using var cursor = await reader.OpenAsync(id, 1);

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert
            .That(async () => await cursor.MoveNextAsync(cancelled.Token))
            .Throws<OperationCanceledException>();
    }

    [Test, Timeout(15000)]
    public async Task BlankLines_NeitherCountedNorPositionedOver(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        // Blank lines are not events: the count, the loader and the cursor must all skip them, and
        // the cursor's byte positioning must not land ON one (positioning counts events, not lines).
        await InjectBlankLinesAsync(id);

        await Assert.That(await reader.GetVersionAsync(id)).IsEqualTo((ulong)Seeded);
        await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(Seeded);

        await using var cursor = await reader.OpenAsync(id, 2);

        await Assert.That(cursor.End).IsEqualTo(3UL);

        var versions = new List<ulong>();
        while (await cursor.MoveNextAsync())
            versions.Add(cursor.CurrentVersion);

        await Assert.That(versions.SequenceEqual([2UL, 3UL])).IsTrue();
    }

    [Test, Timeout(15000)]
    public async Task BlankLines_AfterLastEvent_AreTerminatedAndKeepTheStreamAppendable(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var (store, reader) = Create();
        var id = await SeedAsync(store, Seeded);

        // A trailing blank line is TERMINATED — it is not the torn tail a crash leaves — so the
        // stream still ends at version Seeded, a cursor past the end is empty, and the next append
        // is accepted at that version.
        await InjectBlankLinesAsync(id);

        await using (var past = await reader.OpenAsync(id, Seeded + 1))
        {
            await Assert.That(past.End).IsEqualTo((ulong)Seeded);
            await Assert.That(await past.MoveNextAsync()).IsFalse();
        }

        await Assert
            .That(await store.AppendAsync(id, Seeded, [new SSnapshotAdded(Seeded + 1)]))
            .IsEqualTo((ulong)Seeded + 1);
    }
}
