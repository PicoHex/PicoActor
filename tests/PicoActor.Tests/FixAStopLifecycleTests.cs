namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A4 — stop signalling must survive a loop that already exited.
// SignalStop cancelled the CTS AFTER the loop's finally had disposed it, so Cancel()
// threw ObjectDisposedException, the mailbox writer was never completed (Post kept
// returning true with no reader alive → Ask hangs forever) and the stop was not
// retryable. StopToken had the same ObjectDisposedException exposure.
// ═══════════════════════════════════════════════════════════

internal sealed record FixALifecycleProbe : ICommand;

/// <summary>OnReadyAsync throws → the loop faults and disposes its CTS.</summary>
internal sealed class FixAInitThrowActor : Actor
{
    public FixAInitThrowActor() { }

    /// <summary>Reads the protected StopToken to prove the property never throws.</summary>
    public CancellationToken ExposedStopToken => StopToken;

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override ValueTask OnReadyAsync() =>
        throw new InvalidOperationException("init boom");
}

public sealed class FixAStopLifecycleTests
{
    /// <summary>Drive the actor manually until its loop is dead.</summary>
    private static async Task<(FixAInitThrowActor Actor, Exception? InitError)> DeadActor()
    {
        var actor = new FixAInitThrowActor();
        actor.AttachToSystem(Guid.CreateVersion7());
        actor.SignalReady();

        Exception? initError = null;
        try
        {
            await actor.InitCompletedTask;
        }
        catch (Exception ex)
        {
            initError = ex;
        }

        return (actor, initError);
    }

    /// <summary>
    /// Every stop entry point must be safe after the loop disposed its resources, and
    /// the stop token must stay readable.
    /// </summary>
    [Test]
    public async Task StopPaths_AfterFaultedLoop_DoNotThrowObjectDisposed()
    {
        var (actor, initError) = await DeadActor();
        await Assert.That(initError).IsNotNull();

        // SignalStop: first call and the idempotent repeat.
        Exception? signalError = null;
        try
        {
            actor.SignalStop();
            actor.SignalStop();
        }
        catch (Exception ex)
        {
            signalError = ex;
        }
        await Assert.That(signalError).IsNull();

        // StopToken must be a cached token, never a disposed-CTS access.
        Exception? tokenError = null;
        try
        {
            _ = actor.ExposedStopToken;
        }
        catch (Exception ex)
        {
            tokenError = ex;
        }
        await Assert.That(tokenError).IsNull();

        // StopAsync rethrows the faulted loop's exception (documented) — but never ODE.
        Exception? stopError = null;
        try
        {
            await actor.StopAsync();
        }
        catch (Exception ex)
        {
            stopError = ex;
        }
        await Assert.That(stopError is ObjectDisposedException).IsFalse();

        Exception? disposeError = null;
        try
        {
            await ((IAsyncDisposable)actor).DisposeAsync();
        }
        catch (Exception ex)
        {
            disposeError = ex;
        }
        await Assert.That(disposeError is ObjectDisposedException).IsFalse();
    }

    /// <summary>
    /// Once the loop is gone the mailbox must be closed too — Post returning true with
    /// no reader left alive means every later Ask hangs forever. The loop's own exit
    /// path must close the writer even when nobody called SignalStop.
    /// </summary>
    [Test]
    public async Task LoopExit_ClosesTheMailbox_SoPostFails()
    {
        var (actor, initError) = await DeadActor();
        await Assert.That(initError).IsNotNull();

        // Poll: no stop signal is sent, so only the loop's own finally can close the writer.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (!actor.Post(new Envelope { Command = new FixALifecycleProbe() }))
                break;
            await Task.Delay(10);
        }

        await Assert.That(actor.Post(new Envelope { Command = new FixALifecycleProbe() })).IsFalse();
    }

    /// <summary>
    /// A graceful stop still drains what was already queued (the Complete-before-Cancel
    /// ordering must not turn a stop into a drop).
    /// </summary>
    [Test]
    public async Task GracefulStop_StillDrainsQueuedMessages()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<FixALifecycleProbeActor>(cmd => new FixALifecycleProbeActor(
            (FixALifecycleProbe)cmd
        ));

        var actor = await system.CreateAsync<FixALifecycleProbeActor>(
            new FixALifecycleProbe()
        );

        // Occupy the loop, buffer work behind it, then stop while it is queued.
        var blocking = system.AskAsync<int>(actor.Id, new FixAProbeBlock(actor)).AsTask();
        await actor.BlockEntered.Task;

        var queued = system.AskAsync<int>(actor.Id, new FixAProbeCounted()).AsTask();
        var queued2 = system.AskAsync<int>(actor.Id, new FixAProbeCounted()).AsTask();

        var stop = system.StopAsync(actor.Id).AsTask();
        actor.ReleaseBlock();

        await Assert.That(await blocking).IsEqualTo(0);
        await Assert.That(await queued).IsEqualTo(1);
        await Assert.That(await queued2).IsEqualTo(2);
        await stop;
    }
}

internal sealed record FixAProbeBlock(FixALifecycleProbeActor Owner) : ICommand;

internal sealed record FixAProbeCounted : ICommand;

internal sealed class FixALifecycleProbeActor : Actor
{
    private readonly TaskCompletionSource<bool> _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public readonly TaskCompletionSource<bool> BlockEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _counted;

    public FixALifecycleProbeActor(FixALifecycleProbe cmd)
        : base(cmd) { }

    public FixALifecycleProbeActor() { }

    public void ReleaseBlock() => _release.TrySetResult(true);

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is FixAProbeBlock)
        {
            BlockEntered.TrySetResult(true);
            await _release.Task.ConfigureAwait(false);
            return _counted;
        }

        if (command is FixAProbeCounted)
            _counted++;

        return _counted;
    }
}
