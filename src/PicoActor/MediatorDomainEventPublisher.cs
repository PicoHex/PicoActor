using PicoActor.Abs;
using PicoLog.Abs;
using PicoMediator.Abs;

namespace PicoActor;

/// <summary>
/// IDomainEventPublisher 的 PicoMediator 实现——事件流出的开箱即用通道。
/// 逐事件 Publish&lt;IDomainEvent&gt;(编译期泛型,AOT 安全);事件→命令的翻译是
/// 订阅者(ISubscriber&lt;IDomainEvent&gt;)的业务职责,与 PicoActor 无关。
/// 逐事件独立隔离:一个事件/订阅者失败不影响后续事件发布。
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
                await _publisher.Publish(e).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // 逐事件隔离:订阅者失败(PicoMediator 抛 AggregateException)不中断后续事件。
                // 事件已持久化(发布在 persist+mutate 之后),失败不影响 actor。
                _logger?.Error(
                    $"Event publish failed for {e.GetType().Name} (actor {actorId} v{version}): {ex.Message}"
                );
            }
        }
    }
}
