namespace PicoNode.Actor;

/// <summary>
/// Default in-memory IEventStore implementation.
/// Stores event streams in a ConcurrentDictionary. Lock-free by design:
/// each actor's stream is only written by its own consumption loop (serial),
/// and LoadAsync only occurs when the actor is not in memory (no concurrent writes).
/// </summary>
public sealed class InMemoryEventStore : IEventStore
{
    private readonly ConcurrentDictionary<Guid, List<IDomainEvent>> _streams = new();

    /// <inheritdoc/>
    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        var stream = _streams.GetOrAdd(actorId, _ => []);

        // Lock-free: same actor's writes are serialized by the consumption loop.
        var current = (ulong)stream.Count;
        if (current != expectedVersion)
            throw new ConcurrencyException(actorId, expectedVersion, current);

        stream.AddRange(events);
        return new ValueTask<ulong>((ulong)stream.Count);
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        if (_streams.TryGetValue(actorId, out var stream))
            return new ValueTask<IReadOnlyList<IDomainEvent>>(stream.AsReadOnly());

        return new ValueTask<IReadOnlyList<IDomainEvent>>(Array.Empty<IDomainEvent>());
    }
}
