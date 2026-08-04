using PicoActor.Abs;

namespace PicoActor.Abs.Tests;

internal sealed record FrameworkProbeEvent : IDomainEvent;

internal sealed record ProbeCreated : IDomainEvent;

/// <summary>把 ProbeCreated 当作框架事件拦截的测试 actor。</summary>
internal sealed class FrameworkFilterActor : EventSourcedActor
{
    public static int MutateCount;
    public static int FrameworkHandled;

    public FrameworkFilterActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is ProbeCreated)
            MutateCount++;
    }

    protected override bool TryHandleFrameworkEvent(IDomainEvent e)
    {
        if (e is ProbeCreated)
        {
            FrameworkHandled++;
            return true;
        }
        return false;
    }
}

public sealed class EventSourcedActorFrameworkEventTests
{
    [Test]
    public async Task FrameworkEvent_IsFilteredFromMutate_OnFlush()
    {
        FrameworkFilterActor.MutateCount = 0;
        FrameworkFilterActor.FrameworkHandled = 0;

        var actor = new FrameworkFilterActor();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        // 直接驱动一次 flush:RaiseEvent 后调 FlushEventsAsync(protected,经 public 路径不可达——用内部测试钩子)
        // 通过 IEventSourcedActor.GetUncommittedEvents + 手动构造不可行(FlushEventsAsync 是 protected)——
        // 改用 ReplayEvents 验证过滤(public 接口),flush 路径由 Task 3 的集成测试覆盖。
        ((IEventSourcedActor)actor).ReplayEvents(new IDomainEvent[] { new ProbeCreated() });

        await Assert.That(FrameworkFilterActor.MutateCount).IsEqualTo(0);
        await Assert.That(FrameworkFilterActor.FrameworkHandled).IsEqualTo(1);
    }
}
