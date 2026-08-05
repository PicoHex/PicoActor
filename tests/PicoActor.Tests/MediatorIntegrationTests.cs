using PicoActor.Abs;
using PicoDI;
using PicoMediator.Abs;
using PicoMediator.DI;

namespace PicoActor.Tests;

/// <summary>Typed subscriber (bridge-routed) for the business event. Event-to-command
/// translation is a business-layer concern.</summary>
internal sealed class MedIntegrationStartedSub : ISubscriber<MedIntegrationStarted>
{
    public static readonly List<MedIntegrationStarted> Received = [];

    public ValueTask Handle(MedIntegrationStarted e, CancellationToken ct = default)
    {
        Received.Add(e);
        return default;
    }
}

internal sealed class SagaCompletedSub : ISubscriber<SagaCompleted>
{
    public static readonly List<SagaCompleted> Received = [];

    public ValueTask Handle(SagaCompleted e, CancellationToken ct = default)
    {
        Received.Add(e);
        return default;
    }
}

/// <summary>Framework failure-event subscriber — SagaFailed typed subscription.</summary>
public sealed class SagaFailedSub : ISubscriber<SagaFailed>
{
    public static readonly List<SagaFailed> Received = [];

    public ValueTask Handle(SagaFailed e, CancellationToken ct = default)
    {
        Received.Add(e);
        return default;
    }
}

internal sealed record MedIntegrationCmd(string Name) : ICommand;

internal sealed record MedIntegrationStarted(string Name) : IDomainEvent;

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

/// <summary>Shared static subscriber Received lists — tests in this class must run serially.</summary>
[NotInParallel]
public sealed partial class MediatorIntegrationTests
{
    [Test]
    public async Task EventOutflow_DeclareAndSubscribe_ReachesSubscriber()
    {
        MedIntegrationStartedSub.Received.Clear();
        SagaCompletedSub.Received.Clear();

        // 1. PicoDI container + declare-and-subscribe (Gen scans and auto-registers typed subscribers)
        // 2. AddPicoActor() resolves the registered IMediator inside the ActorSystem factory
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

        // 3. Saga completes → events flow out → typed subscribers receive
        //    (business event + framework terminal event via the Abs bridge)
        await system.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("hello"));
        await Task.Delay(300); // publish runs inside ProcessAsync's flush; delay is defensive

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].Name).IsEqualTo("hello");
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Result).IsEqualTo("hello");
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

        await system.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("root"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
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

        // Child scope disposed: mediator bound to the root scope — outflow unaffected
        // (creation command ignored by the parameterless factory; AskAsync("x") produces one event)
        await system.AskAsync<string>(sagaId, new MedIntegrationCmd("x"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
    }

    [Test]
    public async Task EventOutflow_BasePublishBridge_ReachesTypedSubscribers()
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

        // Adapter publishes Publish<IDomainEvent> (static base type) — the bridge routes
        // to concrete typed subscribers at runtime
        await system.ExecuteSaga<MedIntegrationSaga, string>(new MedIntegrationCmd("hello"));
        await Task.Delay(300);

        await Assert.That(MedIntegrationStartedSub.Received.Count).IsEqualTo(1);
        await Assert.That(MedIntegrationStartedSub.Received[0].Name).IsEqualTo("hello");

        // Abs targets net10.0 — it generates its own bridge, so framework events
        // are typed-subscribable too
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Result).IsEqualTo("hello");
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
        // SagaCompleted is published
        await Task.Delay(300);
        await Assert.That(SagaCompletedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaCompletedSub.Received[0].Result).IsEqualTo("recover");
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

        await Assert
            .That(async () => await system.ExecuteSaga<FailSaga, string>(new FailCmd()))
            .Throws<SagaExecutionException>();

        // Failure = terminal state: SagaFailed event flows out to the typed subscriber
        await Task.Delay(300);
        await Assert.That(SagaFailedSub.Received.Count).IsEqualTo(1);
        await Assert.That(SagaFailedSub.Received[0].Reason).Contains("step failed");
    }
}
