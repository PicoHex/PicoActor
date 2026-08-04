using PicoActor.Abs;

namespace PicoActor.Tests;

internal sealed record DiscardProbeCmd : ICommand;

/// <summary>OnReadyAsync 执行计数——验证丢弃副本不执行 OnReadyAsync。</summary>
internal sealed class DiscardProbeActor : EventSourcedActor
{
    public static int ReadyCount;

    public DiscardProbeActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask OnReadyAsync()
    {
        Interlocked.Increment(ref ReadyCount);
        await base.OnReadyAsync();
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class ActorSystemDiscardTests
{
    [Test]
    public async Task DiscardedActor_SkipsOnReadyAsync()
    {
        DiscardProbeActor.ReadyCount = 0;

        var actor = new DiscardProbeActor();
        actor.Id = Guid.CreateVersion7();
        actor.MarkDiscarded();

        await actor.StopAsync(); // 释放 gate → RunAsync 醒来 → 跳过 OnReadyAsync → 退出

        await Assert.That(DiscardProbeActor.ReadyCount).IsEqualTo(0);
    }

    [Test]
    public async Task AskAsync_ToStoppedActor_FaultsInsteadOfHanging()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<DiscardProbeActor>(_ => new DiscardProbeActor());

        var actor = await system.CreateAsync<DiscardProbeActor>(new DiscardProbeCmd());
        await system.StopAsync(actor.Id);

        // StopAsync 后 registry 已移除 → AskAsync 抛 KeyNotFoundException(loud,不挂起)
        await Assert
            .That(async () => await system.AskAsync<int>(actor.Id, new DiscardProbeCmd()))
            .Throws<KeyNotFoundException>();
    }
}
