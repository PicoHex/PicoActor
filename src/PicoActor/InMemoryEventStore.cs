namespace PicoActor;

/// <summary>
/// Thread-safe in-memory event store for development, testing, and single-process
/// production. Serializes append/load per actor with a semaphore — the version
/// check and append are atomic, so concurrent writers to the same actor stream
/// cannot silently interleave (one wins, the rest throw
/// <see cref="ConcurrencyException"/>). Lock contention is per actor, so
/// different actors never block each other.
/// </summary>
public sealed class InMemoryEventStore : IEventStore, IEventStoreEnumerator, IEventStoreSnapshot
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
}
