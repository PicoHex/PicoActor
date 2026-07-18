namespace PicoNode.Actor.Abs;

/// <summary>
/// Optional Event Sourcing interface. Mailbox auto-persists events after each message.
/// Actors not implementing this interface are pure in-memory operational types.
/// </summary>
public interface IEventSourcedActor : IActor
{
    /// <summary>Current version. Managed by the persistence layer (runtime) and set by ReplayEvents.</summary>
    ulong Version { get; }

    /// <summary>Uncommitted events produced by the current message.</summary>
    IReadOnlyList<IDomainEvent> GetUncommittedEvents();

    /// <summary>Clear uncommitted events after successful persistence. Does NOT touch the mailbox queue.</summary>
    void CommitEvents();

    /// <summary>
    /// Rebuild in-memory state from persisted events.
    /// Called once before the first message is dispatched. Messages queue but don't dispatch during replay.
    /// Does NOT touch the mailbox queue.
    /// </summary>
    void ReplayEvents(IReadOnlyList<IDomainEvent> events);
}
