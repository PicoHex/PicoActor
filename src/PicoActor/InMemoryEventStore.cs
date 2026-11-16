namespace PicoActor;

/// <summary>
/// Thread-safe in-memory event store for development, testing, and single-process
/// production. Serializes append/load per actor with a semaphore — the version
/// check and append are atomic, so concurrent writers to the same actor stream
/// cannot silently interleave (one wins, the rest throw
/// <see cref="ConcurrencyException"/>). Lock contention is per actor, so
/// different actors never block each other.
/// </summary>
public sealed class InMemoryEventStore
    : IEventStore,
        IEventStoreEnumerator,
        IEventStoreSnapshot,
        IEventLogReader
{
    private readonly ConcurrentDictionary<Guid, List<IDomainEvent>> _streams = new();
    private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _locks = new();

    /// <summary>
    /// At most ONE snapshot per actor (the latest saved overwrites the previous one):
    /// snapshots are a recovery accelerator, not history — history is the event stream,
    /// which snapshots never touch.
    /// </summary>
    private readonly ConcurrentDictionary<Guid, ActorSnapshot> _snapshots = new();

    /// <inheritdoc/>
    public async ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        var stream = _streams.GetOrAdd(actorId, _ => []);
        var gate = _locks.GetOrAdd(actorId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            var current = (ulong)stream.Count;
            if (current != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, current);

            stream.AddRange(events);
            return (ulong)stream.Count;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        if (!_streams.TryGetValue(actorId, out var stream))
            return Array.Empty<IDomainEvent>();

        // Snapshot under the gate so a concurrent AddRange cannot be observed
        // mid-write.
        var gate = _locks.GetOrAdd(actorId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return stream.ToList();
        }
        finally
        {
            gate.Release();
        }
    }

    /// <inheritdoc/>
    public async ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    {
        if (!_streams.TryGetValue(actorId, out var stream) || stream.Count == 0)
            return null;

        // Snapshot under the gate so a concurrent AddRange cannot be observed
        // mid-write (symmetric with LoadAsync).
        var gate = _locks.GetOrAdd(actorId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync().ConfigureAwait(false);
        try
        {
            return stream[0];
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// The stream length, read under the same per-actor gate as <see cref="LoadAsync"/> and from
    /// the same list — so the version and the events can never disagree.
    /// </summary>
    public async ValueTask<ulong> GetVersionAsync(Guid actorId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (!_streams.TryGetValue(actorId, out var stream))
            return 0;

        var gate = _locks.GetOrAdd(actorId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return (ulong)stream.Count;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Open a forward-only cursor over the events with version &gt;= <paramref name="fromVersion"/>.
    /// <para>
    /// The stream is COPIED to an array under the per-actor gate and that array is the cursor's
    /// read snapshot: a concurrent <see cref="AppendAsync"/> cannot be observed mid-write and can
    /// never extend a cursor that already reports its <see cref="IEventCursor.End"/> — the same
    /// invariant <see cref="LoadAsync"/>'s <c>ToList()</c> under the gate provides. A version past
    /// the end is legal and yields an empty cursor with the real <see cref="IEventCursor.End"/>.
    /// </para>
    /// </summary>
    public async ValueTask<IEventCursor> OpenAsync(
        Guid actorId,
        ulong fromVersion,
        CancellationToken ct = default
    )
    {
        // 1-based: there is no version 0 to open at.
        if (fromVersion == 0)
            throw new ArgumentOutOfRangeException(
                nameof(fromVersion),
                fromVersion,
                "fromVersion is 1-based."
            );

        ct.ThrowIfCancellationRequested();

        if (!_streams.TryGetValue(actorId, out var stream))
            return new StreamCursor([], fromVersion);

        var gate = _locks.GetOrAdd(actorId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return new StreamCursor(stream.ToArray(), fromVersion);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <inheritdoc/>
    ValueTask<IDomainEvent?> IEventLogReader.PeekFirstAsync(Guid actorId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        // Same cost and same gate discipline as the IEventStore member: one first-element read.
        return PeekFirstAsync(actorId);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The snapshot returned is the one stored here — the SAME array, not a copy, because a
    /// read path may run on every rebuild of an aggregate. It is therefore READ-ONLY for the
    /// caller: writing into <see cref="ActorSnapshot.Payload"/> would silently rewrite the
    /// snapshot every later recovery of this actor restores.
    /// <see cref="SaveSnapshotAsync"/> copies on the way in, so a producer may keep reusing its
    /// serialization buffer.
    /// </remarks>
    public ValueTask<ActorSnapshot?> LoadSnapshotAsync(
        Guid actorId,
        CancellationToken ct = default
    ) => new(_snapshots.TryGetValue(actorId, out var snapshot) ? snapshot : null);

    /// <inheritdoc/>
    public ValueTask SaveSnapshotAsync(
        Guid actorId,
        ActorSnapshot snapshot,
        CancellationToken ct = default
    )
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        // Last write wins: the newest snapshot supersedes every older one for this actor.
        // The payload is COPIED, not referenced: an aggregate is allowed to serialize into a
        // buffer it reuses (that is the whole point of keeping one), and keeping the reference
        // would let the next TakeSnapshot rewrite the snapshot already stored here — a stream
        // description that silently changes under a version it no longer matches.
        _snapshots[actorId] = new ActorSnapshot(snapshot.Version, snapshot.Payload.ToArray());
        return default;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<Guid>> ListAggregateIdsAsync(string firstEventType)
    {
        var result = new List<Guid>();
        foreach (var (id, stream) in _streams)
        {
            // Same gate discipline as Append/Load/Peek: the first-element read
            // must not race a concurrent AddRange's internal resize.
            var gate = _locks.GetOrAdd(id, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (stream.Count > 0 && stream[0].GetType().Name == firstEventType)
                    result.Add(id);
            }
            finally
            {
                gate.Release();
            }
        }
        return result;
    }

    /// <summary>
    /// Read snapshot of one stream: the events copied at open, the length frozen as
    /// <see cref="End"/>, and a 1-based position. It yields at most what the snapshot holds, so a
    /// later append is invisible to it (never a torn or extended read).
    /// </summary>
    private sealed class StreamCursor : IEventCursor
    {
        private readonly IDomainEvent[] _events;

        /// <summary>Index of the next event to yield.</summary>
        private int _next;

        /// <summary>Index of the event <see cref="Current"/> refers to; -1 = none.</summary>
        private int _current = -1;

        internal StreamCursor(IDomainEvent[] events, ulong fromVersion)
        {
            _events = events;

            // fromVersion is 1-based. Past the end is LEGAL (the rollback path opens at
            // snapshot.Version + 1): the cursor is simply empty and End stays the real length.
            var start = fromVersion - 1;
            _next = start >= (ulong)events.Length ? events.Length : (int)start;
        }

        public ulong End => (ulong)_events.Length;

        public IDomainEvent Current => _current >= 0 ? _events[_current] : throw NoCurrent();

        // version == position in the stream, so the index IS the version minus one.
        public ulong CurrentVersion => _current >= 0 ? (ulong)_current + 1 : throw NoCurrent();

        public ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            if (_next >= _events.Length)
            {
                // Past the end: Current stops meaning anything and the cursor stays exhausted.
                _current = -1;
                return new ValueTask<bool>(false);
            }

            _current = _next++;
            return new ValueTask<bool>(true);
        }

        /// <summary>
        /// Idempotent and inert: the cursor holds no handle, so there is nothing to release — and
        /// disposal must not invalidate an event that was already produced.
        /// </summary>
        public ValueTask DisposeAsync() => default;

        private static InvalidOperationException NoCurrent() =>
            new("The cursor has no current event: MoveNextAsync must return true first.");
    }
}
