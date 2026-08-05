using PicoMediator.Abs;

namespace PicoActor.Abs;

/// <summary>
/// Marker interface. All domain events produced by an ES actor must implement this.
/// 事件即通知:IDomainEvent 继承 PicoMediator.Abs.IEvent,可经 PicoMediator 发布与订阅。
/// </summary>
public interface IDomainEvent : IEvent { }
