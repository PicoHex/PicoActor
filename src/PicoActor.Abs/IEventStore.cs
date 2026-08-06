namespace PicoActor.Abs;

/// <summary>
/// Event stream persistence contract. Append-only with optimistic concurrency.
/// Defined in Abs because EventSourcedActor.ProcessAsync needs it — persistence
/// is part of the actor lifecycle, not an external concern.
/// </summary>
public interface IEventStore
{
    /// <summary>
    /// Append events to an actor's event stream.
    /// <paramref name="expectedVersion"/> is the version before these events —
    /// used for optimistic concurrency control. Implementations should throw
    /// ConcurrencyException if the stream has diverged.
    /// Returns the new version after append.
    /// </summary>
    ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    );

    /// <summary>
    /// Load all events for an actor, ordered by version ascending.
    /// Returns empty list if the stream does not exist.
    /// </summary>
    ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId);

    /// <summary>
    /// Read only the FIRST event of a stream, without parsing the remaining
    /// lines. Recovery enumeration (<see cref="IActorSystem.FindAggregateIds"/>)
    /// uses this instead of <see cref="LoadAsync"/> so cost stays O(aggregates),
    /// not O(total events). Returns null when the stream does not exist or
    /// yields no deserializable event.
    /// </summary>
    ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId);
}
