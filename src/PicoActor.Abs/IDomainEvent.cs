using PicoMediator.Abs;

namespace PicoActor.Abs;

/// <summary>
/// Marker interface. All domain events produced by an ES actor must implement this.
/// Events are notifications: IDomainEvent inherits PicoMediator.Abs.IEvent, so it can be
/// published and subscribed to via PicoMediator.
/// </summary>
public interface IDomainEvent : IEvent { }
