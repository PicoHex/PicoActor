namespace PicoActor.Abs;

/// <summary>
/// Domain-event subscriber: receives the typed envelope (event + source aggregate
/// context) and translates it into commands sent to actors via <see cref="ICommandSender"/>.
/// Registered automatically by PicoActor.Gen (declare-and-subscribe) — no manual wiring.
/// </summary>
public interface IDomainEventSubscriber<TEvent>
    where TEvent : IDomainEvent
{
    ValueTask Handle(
        DomainEventEnvelope<TEvent> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    );
}

/// <summary>Typed delivery envelope. <see cref="Event"/> is guaranteed to be <typeparamref name="TEvent"/>
/// (narrowed by the generated envelope bridge).
/// <see cref="Version"/> is the source actor's version AFTER the whole flush batch —
/// every envelope produced by one flush shares it, so it is NOT the individual event's
/// version (a 3-event batch reports the same batch-end version on all three envelopes).</summary>
public sealed record DomainEventEnvelope<TEvent>(Guid ActorId, ulong Version, TEvent Event)
    where TEvent : IDomainEvent;

/// <summary>Transport envelope. Implements IEvent but NOT IDomainEvent — it never
/// enters the event store. Published per event by MediatorDomainEventPublisher;
/// the generated bridge narrows <see cref="Event"/> to its concrete type.
/// <see cref="Version"/> is the batch-end version shared by all events of one flush
/// (see <see cref="DomainEventEnvelope{TEvent}"/>).</summary>
public sealed record DomainEventEnvelope(Guid ActorId, ulong Version, IDomainEvent Event) : IEvent;
