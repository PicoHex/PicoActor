namespace PicoActor.Abs;

/// <summary>
/// One aggregate state snapshot: <paramref name="Payload"/> is whatever
/// <see cref="EventSourcedActor.TakeSnapshot"/> produced for the state reached at
/// <paramref name="Version"/>.
/// <para>
/// <paramref name="Version"/> is the number of events the payload covers (identical to the
/// event-count version model of <see cref="IEventStore"/>): recovery restores the payload and
/// then replays only the events AFTER that version.
/// </para>
/// <para>
/// BUFFER OWNERSHIP: the producer of a payload is allowed to REUSE its serialization buffer (one
/// buffer per aggregate, filled again on the next snapshot), so whoever keeps an
/// <see cref="ActorSnapshot"/> beyond the <see cref="IEventStoreSnapshot.SaveSnapshotAsync"/>
/// call must keep its own copy (the bundled <c>InMemoryEventStore</c> does). Until the array is
/// overwritten the snapshot describes exactly the state at <paramref name="Version"/>.
/// </para>
/// </summary>
public sealed record ActorSnapshot(ulong Version, byte[] Payload);

/// <summary>
/// Optional companion contract for <see cref="IEventStore"/>: snapshot persistence, used as
/// a recovery ACCELERATOR only.
/// <para>
/// The event stream stays complete and authoritative — a snapshot never replaces, truncates
/// or deletes events. Losing every snapshot costs speed (a full replay), never correctness:
/// the framework falls back to replaying the whole stream whenever a snapshot is missing,
/// unreadable, or cannot be applied.
/// </para>
/// <para>
/// A store that does not implement this interface simply disables snapshotting, and an
/// implementation should keep AT MOST ONE snapshot per actor (always the latest): older ones
/// are superseded, not history.
/// </para>
/// </summary>
public interface IEventStoreSnapshot
{
    /// <summary>
    /// Read the latest snapshot for <paramref name="actorId"/>, or null when none was ever
    /// saved (or the store has none for this actor).
    /// </summary>
    ValueTask<ActorSnapshot?> LoadSnapshotAsync(Guid actorId, CancellationToken ct = default);

    /// <summary>
    /// Persist (or replace) the snapshot for <paramref name="actorId"/>.
    /// </summary>
    ValueTask SaveSnapshotAsync(
        Guid actorId,
        ActorSnapshot snapshot,
        CancellationToken ct = default
    );
}

/// <summary>
/// Recovery contract implemented by <see cref="EventSourcedActor"/> and CALLED BY THE RUNTIME
/// (the actor system): it restores a snapshot and replays only the events after it, without
/// widening <see cref="IEventSourcedActor"/> (whose
/// <see cref="IEventSourcedActor.ReplayEvents"/> keeps its "rebuild from the whole stream"
/// meaning). Application code derives from <see cref="EventSourcedActor"/> rather than
/// implementing this interface.
/// </summary>
public interface ISnapshotableActor : IEventSourcedActor
{
    /// <summary>
    /// Restore state from a snapshot payload. <paramref name="version"/> is the version the
    /// payload covers. Throwing is allowed: the framework reports it and falls back to a full
    /// replay.
    /// </summary>
    void RestoreSnapshot(ulong version, byte[] payload);

    /// <summary>
    /// Rebuild state from the events AFTER <paramref name="baseVersion"/>: the resulting
    /// <see cref="IEventSourcedActor.Version"/> is <c>baseVersion + events.Count</c>.
    /// <paramref name="baseVersion"/> = 0 is a full replay (identical to
    /// <see cref="IEventSourcedActor.ReplayEvents"/>).
    /// </summary>
    void ReplayEventsFrom(ulong baseVersion, IReadOnlyList<IDomainEvent> events);
}
