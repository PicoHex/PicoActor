namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// P4 — the shutdown window of StopAllAsync must survive
//      concurrent mass stops: it closes when the LAST call
//      returns, not when the first one does.
// ═══════════════════════════════════════════════════════════

/// <summary>
/// Blocks the actor loop between <see cref="Release"/> and the stop of its own actor.
/// <see cref="StopSignaled"/> completes the moment the actor's own StopToken is cancelled,
/// i.e. the exact point where a mass stop starts waiting for this loop — the test uses it
/// instead of polling the registry.
/// </summary>
internal sealed record FixBWindowBlock(
    TaskCompletionSource<bool> Entered,
    TaskCompletionSource<bool> Release,
    TaskCompletionSource<bool> StopSignaled
) : ICommand;

internal sealed class FixBWindowActor : ActorBase
{
    public FixBWindowActor(FixBCreateCmd cmd)
        : base(cmd) { }

    public FixBWindowActor() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is FixBWindowBlock block)
        {
            // Registered BEFORE entering: a token that is already cancelled fires inline.
            using var registration = StopToken.Register(() =>
                block.StopSignaled.TrySetResult(true)
            );
            block.Entered.TrySetResult(true);
            await block.Release.Task.ConfigureAwait(false);
        }

        return null;
    }
}

public sealed class FixBStopWindowTests
{
    /// <summary>
    /// Two concurrent StopAllAsync calls, each parked on one actor. When the first one
    /// returns, the second is still running — so creation must still be refused and lookups
    /// must still miss. With a 0/1 flag the first returning call closed the window and a
    /// new actor could be created (and handed out) behind the still-running shutdown.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAllAsync_ConcurrentCalls_KeepWindowOpenUntilLastReturns(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<FixBWindowActor>(cmd => new FixBWindowActor((FixBCreateCmd)cmd));

        var first = await system.CreateAsync<FixBWindowActor>(new FixBCreateCmd());
        var second = await system.CreateAsync<FixBWindowActor>(new FixBCreateCmd());

        var firstEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var firstRelease = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var firstStop = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondRelease = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondStop = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        system.Send(first.Id, new FixBWindowBlock(firstEntered, firstRelease, firstStop));
        system.Send(second.Id, new FixBWindowBlock(secondEntered, secondRelease, secondStop));
        await firstEntered.Task;
        await secondEntered.Task;

        // Mass stop #1: it walks the registry in an arbitrary order and parks on the first
        // actor it reaches — whichever one signalled its stop first.
        var stopAll1 = system.StopAllAsync().AsTask();

        var parked1 = await Task.WhenAny(firstStop.Task, secondStop.Task);
        var parked2 = ReferenceEquals(parked1, firstStop.Task) ? secondStop.Task : firstStop.Task;
        var parked1Release = ReferenceEquals(parked1, firstStop.Task)
            ? firstRelease
            : secondRelease;
        var parked2Release = ReferenceEquals(parked2, firstStop.Task)
            ? firstRelease
            : secondRelease;

        // Mass stop #2: the first actor is already out of the registry, so it parks on the
        // other one. Both calls are now in flight, each waiting for a different loop.
        var stopAll2 = system.StopAllAsync().AsTask();
        await parked2;

        // Let call #1 finish its remaining rounds. Call #2 is still parked on its actor.
        parked1Release.TrySetResult(true);
        await stopAll1;

        // The window must still be open: call #2 has not returned yet.
        await Assert
            .That(async () => await system.CreateAsync<FixBWindowActor>(new FixBCreateCmd()))
            .Throws<InvalidOperationException>();
        await Assert.That(await system.GetAsync<FixBWindowActor>(first.Id)).IsNull();
        await Assert.That(await system.GetAsync<FixBWindowActor>(second.Id)).IsNull();

        // Only now does the window close.
        parked2Release.TrySetResult(true);
        await stopAll2;

        var fresh = await system.CreateAsync<FixBWindowActor>(new FixBCreateCmd());
        await Assert.That(fresh.Id).IsNotEqualTo(Guid.Empty);
        await system.StopAllAsync();
    }
}
