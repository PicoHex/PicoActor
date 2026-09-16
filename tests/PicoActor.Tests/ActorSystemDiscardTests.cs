namespace PicoActor.Tests;

internal sealed record DiscardProbeCmd : ICommand;

/// <summary>Counts OnReadyAsync executions — verifies discarded copies never run OnReadyAsync.</summary>
internal sealed class DiscardProbeActor : EventSourcedActor
{
    // Instance field rather than static: tests within a TUnit class run in parallel,
    // so a static counter would be disturbed by AskAsync_ToStoppedActor's CreateAsync
    // (which runs OnReadyAsync normally).
    public int ReadyCount;

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
        var actor = new DiscardProbeActor();
        actor.AttachToSystem(Guid.CreateVersion7());
        actor.MarkDiscarded();

        await actor.StopAsync(); // releases the gate → RunAsync wakes → skips OnReadyAsync → exits

        await Assert.That(actor.ReadyCount).IsEqualTo(0);
    }

    [Test]
    public async Task AskAsync_ToStoppedActor_FaultsInsteadOfHanging()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<DiscardProbeActor>(_ => new DiscardProbeActor());

        var actor = await system.CreateAsync<DiscardProbeActor>(new DiscardProbeCmd());
        await system.StopAsync(actor.Id);

        // After StopAsync the registry entry is removed → AskAsync throws KeyNotFoundException (loud, no hang)
        await Assert
            .That(async () => await system.AskAsync<int>(actor.Id, new DiscardProbeCmd()))
            .Throws<KeyNotFoundException>();
    }
}
