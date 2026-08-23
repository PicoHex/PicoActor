using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>
/// Contract tests for IActorSystem.RequestStop — the synchronous, loop-safe
/// stop primitive used by the saga terminal path (replaces the fire-and-forget
/// Task.Run + Task.Yield workaround).
/// </summary>
public sealed class ActorSystemRequestStopTests
{
    private sealed record StopSelfCmd : ICommand;

    private sealed class StopSelfActor : Actor
    {
        public StopSelfActor() { }

        public StopSelfActor(StopSelfCmd cmd)
            : base(cmd) { }

        protected override ValueTask<object?> OnMessageAsync(ICommand command)
        {
            if (command is StopSelfCmd && System is not null)
                System.RequestStop(Id);
            return default;
        }
    }

    [Test]
    public async Task RequestStop_RemovesFromRegistry_AskThrowsKeyNotFound()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<StopSelfActor>(_ => new StopSelfActor(), () => new StopSelfActor());

        var actor = await system.CreateAsync<StopSelfActor>(new StopSelfCmd());

        system.RequestStop(actor.Id);

        await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new StopSelfCmd()))
            .Throws<KeyNotFoundException>();
    }

    [Test]
    public async Task RequestStop_IsIdempotent_AndInteropsWithStopAsync()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<StopSelfActor>(_ => new StopSelfActor(), () => new StopSelfActor());

        var actor = await system.CreateAsync<StopSelfActor>(new StopSelfCmd());

        system.RequestStop(actor.Id);
        system.RequestStop(actor.Id); // second call: no-op
        await system.StopAsync(actor.Id); // interop: no-op, must not hang or throw
        system.RequestStop(actor.Id); // after StopAsync: still no-op

        await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new StopSelfCmd()))
            .Throws<KeyNotFoundException>();
    }

    [Test]
    public async Task RequestStop_LoopTerminates_DisposeCompletes()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<StopSelfActor>(_ => new StopSelfActor(), () => new StopSelfActor());

        var actor = await system.CreateAsync<StopSelfActor>(new StopSelfCmd());

        system.RequestStop(actor.Id);

        // DisposeAsync awaits the loop task — completion proves the loop exited
        // (the 5s guard turns a stuck loop into a test failure instead of a hang).
        var dispose = ((IAsyncDisposable)actor).DisposeAsync().AsTask();
        var winner = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(5)));
        await Assert.That(ReferenceEquals(winner, dispose)).IsTrue();
    }

    [Test]
    public async Task RequestStop_FromWithinActorTurn_IsLoopSafe()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<StopSelfActor>(_ => new StopSelfActor(), () => new StopSelfActor());

        var actor = await system.CreateAsync<StopSelfActor>(new StopSelfCmd());

        // The message turn calls System.RequestStop(Id) synchronously — the exact
        // scenario that used to require the Task.Run + Task.Yield escape hatch.
        // Must not deadlock: the Ask completes, and the registry is already empty.
        await system.AskAsync<object?>(actor.Id, new StopSelfCmd());

        await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new StopSelfCmd()))
            .Throws<KeyNotFoundException>();
    }
}
