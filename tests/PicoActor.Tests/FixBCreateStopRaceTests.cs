namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// B3 — CreateAsync must never return an actor that was
//      stopped while its initialization was in flight.
// ═══════════════════════════════════════════════════════════

internal sealed record FixBCreateCmd : ICommand;

/// <summary>Actor whose initialization parks on a gate, so a stop can land mid-init.</summary>
internal sealed class FixBStopRaceActor : ActorBase
{
    private readonly TaskCompletionSource<bool> _initEntered;
    private readonly TaskCompletionSource<bool> _releaseInit;

    public FixBStopRaceActor(
        FixBCreateCmd cmd,
        TaskCompletionSource<bool> initEntered,
        TaskCompletionSource<bool> releaseInit
    )
        : base(cmd)
    {
        _initEntered = initEntered;
        _releaseInit = releaseInit;
    }

    /// <summary>True once initialization ran to completion.</summary>
    public bool Ready { get; private set; }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask OnReadyAsync()
    {
        _initEntered.TrySetResult(true);
        await _releaseInit.Task.ConfigureAwait(false);
        Ready = true;
    }
}

public sealed class FixBCreateStopRaceTests
{
    /// <summary>
    /// A concurrent StopAsync removes the registry entry while the new actor is still
    /// initializing. Its initialization then completes successfully, but the actor is
    /// already out of the system — CreateAsync must fail loudly instead of returning a
    /// live-looking handle to a dead actor.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task CreateAsync_ActorStoppedDuringInitialization_Throws(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );

        var initEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseInit = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        FixBStopRaceActor? instance = null;
        system.Register<FixBStopRaceActor>(cmd =>
            instance = new FixBStopRaceActor((FixBCreateCmd)cmd, initEntered, releaseInit)
        );

        var createTask = Task.Run(() =>
            system.CreateAsync<FixBStopRaceActor>(new FixBCreateCmd()).AsTask()
        );

        await initEntered.Task; // registered, Id assigned, initialization in flight
        var id = instance!.Id;

        // Stop lands mid-init: the registry entry is removed synchronously, the loop has to
        // wait for the initialization to finish before it can exit.
        var stopTask = system.StopAsync(id).AsTask();
        await Assert
            .That(() => system.Send(id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();

        releaseInit.TrySetResult(true);

        Exception? failure = null;
        try
        {
            await createTask;
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        await Assert.That(failure).IsNotNull();
        await Assert.That(failure).IsTypeOf<InvalidOperationException>();
        await Assert
            .That(
                failure!.Message.Contains(
                    "was stopped during initialization",
                    StringComparison.Ordinal
                )
            )
            .IsTrue();

        await stopTask;
    }
}
