namespace PicoActor;

/// <summary>
/// PicoMediator implementation of IDomainEventPublisher — the out-of-the-box
/// channel for event outflow.
/// Publishes each event wrapped in a <see cref="DomainEventEnvelope"/> (concrete
/// transport type carrying the source aggregate context: ActorId + Version);
/// event→command translation is the business responsibility of subscribers
/// (IDomainEventSubscriber&lt;TEvent&gt;), unrelated to PicoActor.
/// Per-event isolation: a failure in one event/subscriber does not affect
/// publishing of subsequent events.
/// </summary>
public sealed class MediatorDomainEventPublisher : IDomainEventPublisher
{
    private readonly IPublisher _publisher;
    private readonly ILogger? _logger;

    public MediatorDomainEventPublisher(IPublisher publisher, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        _publisher = publisher;
        _logger = logger;
    }

    public async ValueTask PublishAsync(
        Guid actorId,
        ulong version,
        IReadOnlyList<IDomainEvent> events
    )
    {
        foreach (var e in events)
        {
            try
            {
                // Concrete-type publish (DomainEventEnvelope) — direct key match,
                // no base-type bridge needed. The generated PicoActor bridge
                // narrows envelope.Event to its concrete type for subscribers.
                await _publisher
                    .Publish(new DomainEventEnvelope(actorId, version, e))
                    .ConfigureAwait(false);
            }
            catch (ObjectDisposedException ex)
            {
                // Captive-dependency diagnostic: the Mediator is bound to a disposed
                // scope (the location where ActorSystem was first resolved).
                // Fix direction: PicoDI singleton factories should always create from
                // the root scope.
                Report(
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): "
                        + $"{ex.Message}. The IMediator is bound to a disposed scope — resolve "
                        + "IActorSystem from the application-level root scope."
                );
            }
            catch (Exception ex)
            {
                // Per-event isolation: a subscriber failure (PicoMediator throws
                // AggregateException) does not interrupt subsequent events.
                // Events are already persisted (publishing happens after persist+mutate),
                // so the failure does not affect the actor.
                Report(
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): {ex.Message}"
                );
            }
        }
    }

    /// <summary>
    /// Report a publish failure. Without a logger the diagnostic still goes to stderr:
    /// the events are durable but downstream subscribers missed them, so a silent
    /// swallow would hide a real delivery gap.
    /// </summary>
    private void Report(string message)
    {
        if (_logger is not null)
            _logger.Error(message);
        else
            Console.Error.WriteLine($"[PicoActor] {message}");
    }
}
