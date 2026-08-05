using PicoActor.Abs;
using PicoDI;

namespace PicoActor.Tests;

internal sealed record DiProbeCmd : ICommand;

internal sealed record DiProbeEvent(int Value) : IDomainEvent;

internal sealed class DiProbeActor : EventSourcedActor
{
    public DiProbeActor() { }

    public DiProbeActor(DiProbeCmd cmd)
        : base(cmd) { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is DiProbeCmd)
            RaiseEvent(new DiProbeEvent(1));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class PicoActorDiExtensionsTests
{
    [Test]
    public async Task AddPicoActor_WithPublisher_WiresEventOutflow()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var publisher = new RecordingMediatorPublisher();
        container.AddPicoActor(publisher);
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<DiProbeActor>(
            cmd => new DiProbeActor((DiProbeCmd)cmd),
            () => new DiProbeActor()
        );

        var actor = await system.CreateAsync<DiProbeActor>(new DiProbeCmd());
        await system.AskAsync<object?>(actor.Id, new DiProbeCmd());

        // 事件已流出到 publisher:构造期 flush + mailbox 命令各一批
        await Assert.That(publisher.Published.Count).IsEqualTo(2);
        await Assert.That(publisher.Published[0]).IsTypeOf<DiProbeEvent>();
        await Assert.That(publisher.Published[1]).IsTypeOf<DiProbeEvent>();
    }
}
