namespace PicoActor.Abs;

/// <summary>
/// Optional Event Sourcing interface. Mailbox auto-persists events after each message.
/// Actors not implementing this interface are pure in-memory operational types.
/// </summary>
public interface IEventSourcedActor : IActor
{
    /// <summary>Current version. Managed by the persistence layer (runtime) and set by ReplayEvents.</summary>
    ulong Version { get; }

    /// <summary>
    /// Clear uncommitted events after successful persistence. Does NOT touch the mailbox queue.
    /// Cleanup-only, NOT a discard primitive: it drops the pending list but leaves
    /// <see cref="Version"/> untouched. Discarding an unpersisted batch (a handler that
    /// decided not to commit) is the runtime's job — raise nothing, or let the dispatch throw
    /// so the framework restores both the list and the version. Calling this for events that
    /// were never persisted inflates Version and makes the next flush fail with
    /// ConcurrencyException.
    /// </summary>
    void CommitEvents();

    /// <summary>
    /// Rebuild in-memory state from persisted events.
    /// Called once before the first message is dispatched. Messages queue but don't dispatch during replay.
    /// Does NOT touch the mailbox queue.
    /// </summary>
    void ReplayEvents(IReadOnlyList<IDomainEvent> events);
}
