using PicoActor.Abs;
using PicoLog.Abs;
using PicoMediator.Abs;

namespace PicoActor;

/// <summary>
/// PicoMediator implementation of IDomainEventPublisher — the out-of-the-box
/// channel for event outflow.
/// Publishes each event via Publish&lt;IDomainEvent&gt; (compile-time generic, AOT-safe);
/// event→command translation is the business responsibility of subscribers
/// (ISubscriber&lt;IDomainEvent&gt;), unrelated to PicoActor.
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
#pragma warning disable PMGEN001 // Cross-assembly base-type publish: concrete events live in the app assembly; the bridge is generated when the app assembly compiles
                await _publisher.Publish(e).ConfigureAwait(false);
#pragma warning restore PMGEN001
            }
            catch (ObjectDisposedException ex)
            {
                // Captive-dependency diagnostic: the Mediator is bound to a disposed
                // scope (the location where ActorSystem was first resolved).
                // Without a logger, the diagnostic still goes to stderr so event outflow
                // never fails silently (events are already persisted — no data loss —
                // but downstream subscribers miss them). Fix direction: PicoDI singleton
                // factories should always create from the root scope.
                var message =
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): "
                    + $"{ex.Message}. The IMediator is bound to a disposed scope — resolve "
                    + "IActorSystem from the application-level root scope.";
                if (_logger is not null)
                    _logger.Error(message);
                else
                    Console.Error.WriteLine($"[PicoActor] {message}");
            }
            catch (Exception ex)
            {
                // Per-event isolation: a subscriber failure (PicoMediator throws
                // AggregateException) does not interrupt subsequent events.
                // Events are already persisted (publishing happens after persist+mutate),
                // so the failure does not affect the actor.
                _logger?.Error(
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): {ex.Message}"
                );
            }
        }
    }
}
