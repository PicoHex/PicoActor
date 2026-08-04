namespace PicoActor.Abs;

/// <summary>
/// Domain-event publish hook: called after events are persisted and mutated
/// (at the end of FlushEventsAsync).
/// Not wired by default; the application injects an implementation
/// (e.g. a PicoMediator fanout).
/// Implementations must not throw to break the actor — the framework isolates
/// failures a second time; failures are recorded by the implementation itself.
/// </summary>
public interface IDomainEventPublisher
{
    /// <summary>
    /// Publish a batch of events for one actor, already durable in the event store.
    /// <paramref name="version"/> is the actor version AFTER this batch was applied.
    /// Replay (recovery) never reaches this hook.
    /// </summary>
    ValueTask PublishAsync(Guid actorId, ulong version, IReadOnlyList<IDomainEvent> events);
}
