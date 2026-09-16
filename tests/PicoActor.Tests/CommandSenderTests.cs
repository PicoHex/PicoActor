namespace PicoActor.Tests;

internal sealed record Echo(string Msg) : ICommand;

internal sealed record Ping(string Msg) : ICommand;

internal sealed record Probe : ICommand;

internal sealed class EchoActor : Actor
{
    private string _last = "";

    public EchoActor(Echo cmd)
        : base(cmd) => _last = cmd.Msg;

    public EchoActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is Echo e)
        {
            _last = e.Msg;
            return new ValueTask<object?>(_last);
        }
        if (command is Ping p)
        {
            _last = p.Msg;
            return default;
        }
        if (command is Probe)
            return new ValueTask<object?>(_last); // read-only probe — returns the last value without modifying state
        return default;
    }
}

internal sealed record MiniSagaCmd : ICommand;

internal sealed record MiniSagaDone(string Name) : IDomainEvent;

internal sealed class MiniSaga : SagaActor
{
    public MiniSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is MiniSagaCmd)
        {
            RaiseEvent(new MiniSagaDone("mini"));
            MarkComplete("done");
            return new ValueTask<object?>("done");
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class CommandSenderTests
{
    [Test]
    public async Task Send_DeliversToMailbox()
    {
        var system = NewSystem();
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        var sender = new ActorSystemCommandSender(system);
        sender.Send(actor.Id, new Ping("pong"));

        var last = await system.AskAsync<string>(actor.Id, new Probe());
        await Assert.That(last).IsEqualTo("pong");
    }

    [Test]
    public async Task AskAsync_ReturnsResult()
    {
        var system = NewSystem();
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        var sender = new ActorSystemCommandSender(system);
        var result = await sender.AskAsync<string>(actor.Id, new Echo("hi"));

        await Assert.That(result).IsEqualTo("hi");
    }

    [Test]
    public async Task ExecuteSaga_RunsSagaToCompletion()
    {
        var system = NewSystem();
        system.Register<MiniSaga>(_ => new MiniSaga(), () => new MiniSaga());

        var sender = new ActorSystemCommandSender(system);
        var execution = await sender.ExecuteSaga<MiniSaga, string>(new MiniSagaCmd());

        await Assert.That(execution.Result).IsEqualTo("done");
        await Assert.That(execution.Id).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task AddPicoActor_RegistersSender_ResolvableFromChildScope()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var childScope = container.CreateScope();

        var sender = (ICommandSender)childScope.GetService(typeof(ICommandSender));
        await Assert.That(sender).IsNotNull();

        var system = (IActorSystem)childScope.GetService(typeof(IActorSystem));
        system.Register<EchoActor>(_ => new EchoActor(), () => new EchoActor());
        var actor = await system.CreateAsync<EchoActor>(new Echo("init"));

        sender.Send(actor.Id, new Ping("via-di"));
        var last = await system.AskAsync<string>(actor.Id, new Probe());
        await Assert.That(last).IsEqualTo("via-di");
    }

    private static IActorSystem NewSystem() =>
        new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
}
