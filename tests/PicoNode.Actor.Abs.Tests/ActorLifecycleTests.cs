using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Abs.Tests;

internal sealed record NoOpCommand : ICommand;

/// <summary>Actor that does nothing — for testing lifecycle.</summary>
internal sealed class NoOpActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;
}

public sealed class ActorLifecycleTests
{
    /// <summary>
    /// Actor should implement IAsyncDisposable for async cleanup.
    /// </summary>
    [Test]
    public async Task Actor_implements_IAsyncDisposable()
    {
        var actor = new NoOpActor();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        await Assert.That(actor).IsAssignableTo<IAsyncDisposable>();
    }

    /// <summary>
    /// StopAsync must be idempotent — safe to call multiple times.
    /// </summary>
    [Test]
    public async Task StopAsync_is_idempotent()
    {
        var actor = new NoOpActor();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        actor.Post(new Envelope { Command = new NoOpCommand() });
        await Task.Delay(100);

        await actor.StopAsync();

        // Second call must NOT throw
        await actor.StopAsync();
    }
}
