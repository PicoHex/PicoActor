namespace PicoActor.Tests;

internal sealed record FailCmd : ICommand;

internal sealed class FailSaga : SagaActor
{
    public FailSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is FailCmd)
            throw new InvalidOperationException("step failed");
        return default;
    }

    protected override async ValueTask ResumeAsync()
    {
        await Task.CompletedTask;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class ExecuteSagaApiTests
{
    [Test]
    public async Task ExecuteSaga_ReturnsIdAndResult_AndAutoStops()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        system.Register<TestSaga>(_ => new TestSaga(), () => new TestSaga());

        var execution = await system.ExecuteSaga<TestSaga, string>(new StartSaga("hello"));

        await Assert.That(execution.Result).IsEqualTo("hello");
        await Assert.That(execution.Id).IsNotEqualTo(Guid.Empty);

        // Complete-then-die (auto-stop is async; wait for it)
        await Task.Delay(300);
        var gone = await system.GetAsync<TestSaga>(execution.Id);
        await Assert.That(gone).IsNull();
    }

    [Test]
    public async Task ExecuteSaga_Failure_ThrowsSagaExecutionException_WithId()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FailSaga>(_ => new FailSaga(), () => new FailSaga());

        var ex = await Assert
            .That(async () => await system.ExecuteSaga<FailSaga, string>(new FailCmd()))
            .Throws<SagaExecutionException>();

        await Assert.That(ex!.SagaId).IsNotEqualTo(Guid.Empty);
        await Assert.That(ex.Reason).Contains("step failed");

        // Failure = terminal: the stream contains the framework SagaFailed; GetAsync does not resurrect
        await Task.Delay(300);
        var gone = await system.GetAsync<FailSaga>(ex.SagaId);
        await Assert.That(gone).IsNull();

        var events = await store.LoadAsync(ex.SagaId);
        await Assert.That(events.Count).IsEqualTo(1);
        await Assert.That(events[0]).IsTypeOf<SagaFailed>();
    }
}
