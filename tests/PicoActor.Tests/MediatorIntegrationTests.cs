using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

/// <summary>Business-event subscriber (declare-and-subscribe via PicoActor.Gen) —
/// receives the typed envelope with the source aggregate context.</summary>
internal sealed class MedIntegrationStartedSub : IDomainEventSubscriber<MedIntegrationStarted>
{
    public static readonly List<DomainEventEnvelope<MedIntegrationStarted>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<MedIntegrationStarted> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

internal sealed class SagaCompletedSub : IDomainEventSubscriber<SagaCompleted>
{
    public static readonly List<DomainEventEnvelope<SagaCompleted>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<SagaCompleted> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

/// <summary>Framework failure-event subscriber — SagaFailed typed subscription.</summary>
public sealed class SagaFailedSub : IDomainEventSubscriber<SagaFailed>
{
    public static readonly List<DomainEventEnvelope<SagaFailed>> Received = [];

    public ValueTask Handle(
        DomainEventEnvelope<SagaFailed> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Received.Add(envelope);
        return default;
    }
}

/// <summary>Event→command translation: Send to a target actor.</summary>
internal sealed class TriggerSendHandler : IDomainEventSubscriber<MedIntegrationTrigger>
{
    public static Guid TargetActorId { get; set; }
    public static readonly List<Guid> Sent = [];

    public ValueTask Handle(
        DomainEventEnvelope<MedIntegrationTrigger> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        sender.Send(TargetActorId, new TargetCmd("ping"));
        Sent.Add(envelope.ActorId);
        return default;
    }
}

/// <summary>Event→command translation: ExecuteSaga.</summary>
internal sealed class TriggerSagaHandler : IDomainEventSubscriber<MedIntegrationTrigger>
{
    public static readonly List<SagaExecution<string>> Results = [];

    public async ValueTask Handle(
        DomainEventEnvelope<MedIntegrationTrigger> envelope,
        ICommandSender sender,
        CancellationToken ct = default
    )
    {
        Results.Add(
            await sender.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("from-trigger"))
        );
    }
}

internal sealed record MedIntegrationCmd(string Name) : ICommand;

internal sealed record MedIntegrationStarted(string Name) : IDomainEvent;

internal sealed record MedIntegrationTrigger(Guid Id) : IDomainEvent;

internal sealed record MedIntegrationUnobserved(string Name) : IDomainEvent;

internal sealed record TriggerCmd : ICommand;

internal sealed record TargetCmd(string Payload) : ICommand;

internal sealed record UnobservedCmd(string Name) : ICommand;

/// <summary>Raises MedIntegrationTrigger — the event handlers translate to commands.</summary>
internal sealed class MedIntegrationTriggerActor : EventSourcedActor
{
    public MedIntegrationTriggerActor(TriggerCmd cmd) : base(cmd) { }
    public MedIntegrationTriggerActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is TriggerCmd)
            RaiseEvent(new MedIntegrationTrigger(Id));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>Target actor for the event→command Send translation.</summary>
internal sealed class MedIntegrationTargetActor : Actor
{
    public static readonly List<string> ReceivedPayloads = [];

    public MedIntegrationTargetActor(TargetCmd cmd) : base(cmd)
    {
        ReceivedPayloads.Add(cmd.Payload);
    }

    public MedIntegrationTargetActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is TargetCmd c)
        {
            ReceivedPayloads.Add(c.Payload);
            return new ValueTask<object?>(c.Payload);
        }
        return default;
    }
}

/// <summary>Raises an event nobody subscribes to — must be silently dropped.</summary>
internal sealed class MedIntegrationUnobservedActor : EventSourcedActor
{
    public MedIntegrationUnobservedActor(UnobservedCmd cmd) : base(cmd) { }
    public MedIntegrationUnobservedActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is UnobservedCmd c)
            RaiseEvent(new MedIntegrationUnobserved(c.Name));
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

internal sealed class MedIntegrationSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public MedIntegrationSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is MedIntegrationCmd c)
        {
            if (_step < 1)
            {
                RaiseEvent(new MedIntegrationStarted(c.Name));
                _name = c.Name;
            }
            MarkComplete(_name); // Unconditional: resumes converge to terminal even when all steps are done
            return new ValueTask<object?>(_name);
        }
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        // Re-dispatch the driving command; step guards skip completed steps and
        // MarkComplete converges the saga to the Completed terminal state.
        await OnMessageAsync(new MedIntegrationCmd(_name));
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is MedIntegrationStarted e)
        {
            _step = 1;
            _name = e.Name;
        }
    }
}

/// <summary>Shared static subscriber lists — tests in this class must run serially.</summary>
[NotInParallel]
public sealed partial class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // Saga completes → events flow out as envelopes → typed subscribers receive
        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("hello")
        );
        await Task.Delay(300); // publish runs inside ProcessAsync's flush; delay is defensive

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        var started = MedIntegrationStartedSub.Received[0];
        await Assert.That(started.ActorId).IsEqualTo(execution.Id); // source aggregate context
        await Assert.That(started.Version).IsEqualTo(2ul); // batch [Started, SagaCompleted] — version after batch
        await Assert.That(started.Event.Name).IsEqualTo("hello");

        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].ActorId).IsEqualTo(execution.Id);
        await Assert.That(SagaCompletedSub.Received[0].Version).IsEqualTo(2ul);
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("hello");
    }

    [Test]
    public async Task AutoWiring_RootScopeBinding_SurvivesChildScopeDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // Recommended usage: ActorSystem first resolved from an application-level (root) scope
        await using var rootScope = container.CreateScope();
        var system = (IActorSystem)rootScope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // Short-lived child scopes do not affect root-bound outflow
        await using (var childScope = container.CreateScope()) { }

        var execution = await system.ExecuteSaga<MedIntegrationSaga, string>(
            new MedIntegrationCmd("root")
        );
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].ActorId).IsEqualTo(execution.Id);
        await Assert.That(MedIntegrationStartedSub.Received[0].Event.Name).IsEqualTo("root");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task AutoWiring_ChildScopeFirstResolution_SurvivesDisposal()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();

        // E1 (PicoDI 2026.8.1): singleton factories use the container-internal root scope —
        // first resolution from a child scope is safe (pre-E1: outflow died after scope disposal)
        IActorSystem system;
        Guid sagaId;
        await using (var childScope = container.CreateScope())
        {
            system = (IActorSystem)childScope.GetService(typeof(IActorSystem));
            system.Register<MedIntegrationSaga>(
                _ => new MedIntegrationSaga(),
                () => new MedIntegrationSaga()
            );
            var saga = await system.CreateAsync<MedIntegrationSaga>(new MedIntegrationCmd("init"));
            sagaId = saga.Id;
        }

        // Child scope disposed: mediator bound to the root scope — outflow unaffected.
        // (creation command ignored by the parameterless factory; AskAsync("x") produces one event)
        await system.AskAsync<string>(sagaId, new MedIntegrationCmd("x"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].ActorId).IsEqualTo(sagaId);
        await Assert.That(MedIntegrationStartedSub.Received[0].Version).IsEqualTo(2ul);
        await Assert.That(MedIntegrationStartedSub.Received[0].Event.Name).IsEqualTo("x");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Recovery_CompletedEvents_FlowToSubscribers()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        // Interrupted saga: step 1 persisted, no terminal event. The explicit store is
        // passed to AddPicoActor so the ActorSystem sees the same stream (AddPicoActor()
        // without an argument would create a fresh InMemoryEventStore).
        var store = new InMemoryEventStore();
        var sagaId = Guid.CreateVersion7();
        await store.AppendAsync(sagaId, 0, [new MedIntegrationStarted("recover")]);

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor(store);
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        var results = await system.ResumeInterruptedSagasAsync<MedIntegrationSaga>(
            nameof(MedIntegrationStarted)
        );
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Status).IsEqualTo(SagaResumeStatus.Completed);

        // Events produced by the resume flow out through the mediator:
        // replay is silent (MedIntegrationStarted not republished), the resume-advanced
        // SagaCompleted is published as an envelope
        await Task.Delay(300);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].ActorId).IsEqualTo(sagaId);
        await Assert.That(SagaCompletedSub.Received[0].Version).IsEqualTo(2ul); // appended after v1 stream
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("recover");
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(0);
    }

    [Test]
    public async Task SagaFailed_FlowsToSubscribers()
    {
        SagaFailedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<FailSaga>(_ => new FailSaga(), () => new FailSaga());

        SagaExecutionException? caught = null;
        try
        {
            await system.ExecuteSaga<FailSaga, string>(new FailCmd());
        }
        catch (SagaExecutionException ex)
        {
            caught = ex;
        }
        await Assert.That(caught).IsNotNull();

        // Failure = terminal state: SagaFailed event flows out to the typed subscriber
        await Task.Delay(300);
        await Assert.That(SagaFailedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaFailedSub.Received[0].ActorId).IsEqualTo(caught!.SagaId);
        await Assert.That(SagaFailedSub.Received[0].Version).IsEqualTo(1ul);
        await Assert.That(SagaFailedSub.Received[0].Event.Reason).Contains("step failed");
    }

    [Test]
    public async Task Handler_EventToCommand_SendReachesTargetActor()
    {
        TriggerSendHandler.Sent.Clear();
        MedIntegrationTargetActor.ReceivedPayloads.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationTriggerActor>(
            cmd => new MedIntegrationTriggerActor((TriggerCmd)cmd),
            () => new MedIntegrationTriggerActor()
        );
        system.Register<MedIntegrationTargetActor>(
            _ => new MedIntegrationTargetActor(),
            () => new MedIntegrationTargetActor()
        );

        var target = await system.CreateAsync<MedIntegrationTargetActor>(new TargetCmd("init"));
        TriggerSendHandler.TargetActorId = target.Id;
        MedIntegrationTargetActor.ReceivedPayloads.Clear(); // discard creation entries (base ctor dispatches OnMessageAsync, derived ctor adds again)
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        ); // keep the sibling TriggerSagaHandler clean (it also fires for MedIntegrationTrigger)

        // creation command is processed → MedIntegrationTrigger fires → handler Sends
        var trigger = await system.CreateAsync<MedIntegrationTriggerActor>(new TriggerCmd());
        await system.AskAsync<object?>(trigger.Id, new TriggerCmd()); // second trigger
        await Task.Delay(300);

        await Assert.That(TriggerSendHandler.Sent.Count).IsEqualTo(2); // two events, envelope.ActorId = source
        await Assert.That(TriggerSendHandler.Sent[0]).IsEqualTo(trigger.Id);
        await Assert.That(MedIntegrationTargetActor.ReceivedPayloads.Count).IsEqualTo(2); // two "ping" deliveries only
        await Assert.That(MedIntegrationTargetActor.ReceivedPayloads).Contains("ping");
    }

    [Test]
    public async Task Handler_EventToCommand_ExecuteSagaRunsSaga()
    {
        TriggerSagaHandler.Results.Clear();
        SagaCompletedSub.Received.Clear();
        MedIntegrationStartedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationTriggerActor>(
            cmd => new MedIntegrationTriggerActor((TriggerCmd)cmd),
            () => new MedIntegrationTriggerActor()
        );
        system.Register<MedIntegrationSaga>(
            _ => new MedIntegrationSaga(),
            () => new MedIntegrationSaga()
        );

        // creation command is processed in the constructor → MedIntegrationTrigger fires → handler ExecuteSaga
        await system.CreateAsync<MedIntegrationTriggerActor>(new TriggerCmd());
        await Task.Delay(300);

        await Assert.That(TriggerSagaHandler.Results.Count).IsEqualTo(1);
        await Assert.That(TriggerSagaHandler.Results[0].Result).IsEqualTo("from-trigger");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Event.Result).IsEqualTo("from-trigger");
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task UnobservedEvent_SilentlyDropped()
    {
        MedIntegrationStartedSub.Received.Clear();

        var container = new SvcContainer(autoConfigureFromGenerator: false);
        container.AddPicoMediator();
        container.AddPicoActor();
        container.Build();
        await using var scope = container.CreateScope();

        var system = (IActorSystem)scope.GetService(typeof(IActorSystem));
        system.Register<MedIntegrationUnobservedActor>(
            _ => new MedIntegrationUnobservedActor(),
            () => new MedIntegrationUnobservedActor()
        );

        var actor = await system.CreateAsync<MedIntegrationUnobservedActor>(
            new UnobservedCmd("ghost")
        );
        await system.AskAsync<object?>(actor.Id, new UnobservedCmd("ghost2"));
        await Task.Delay(300);

        // no subscribers for MedIntegrationUnobserved — everything completes without error
        await Assert.That(actor.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(0); // no leakage
    }
}
