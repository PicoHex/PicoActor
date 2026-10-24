namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A3 — cancellation caused by the actor being STOPPED is a shutdown interrupt, not a
// business failure. Recording SagaFailed for it permanently buries an in-flight saga:
// one normal restart turns it into a terminal, never-resumable record. Cancellation
// from a caller-owned token (a timeout) keeps the existing business-failure semantics.
// ═══════════════════════════════════════════════════════════

internal sealed record FixASagaCreate : ICommand;

internal sealed record FixAWaitForStop : ICommand;

internal sealed record FixAWaitForUserToken(CancellationTokenSource Source) : ICommand;

internal sealed record FixACompleteThenWaitForStop : ICommand;

internal sealed class FixACancelSaga : SagaActor
{
    public readonly TaskCompletionSource<bool> Entered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _step;

    public FixACancelSaga() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixASagaCreate:
                _step = 1;
                return null;
            case FixAWaitForStop:
                Entered.TrySetResult(true);
                await Task.Delay(Timeout.Infinite, StopToken).ConfigureAwait(false);
                return null;
            case FixAWaitForUserToken t:
                Entered.TrySetResult(true);
                await Task.Delay(Timeout.Infinite, t.Source.Token).ConfigureAwait(false);
                return null;
            case FixACompleteThenWaitForStop:
                MarkComplete("premature");
                Entered.TrySetResult(true);
                await Task.Delay(Timeout.Infinite, StopToken).ConfigureAwait(false);
                return null;
            default:
                return null;
        }
    }

    // No step advance on recovery — this saga only models in-flight work.
    protected override ValueTask ResumeAsync() => default;

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class FixASagaCancellationTests
{
    private static async Task<(ActorSystem System, InMemoryEventStore Store, FixACancelSaga Saga)> NewSaga()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixACancelSaga>(_ => new FixACancelSaga(), () => new FixACancelSaga());
        var saga = await system.CreateAsync<FixACancelSaga>(new FixASagaCreate());
        return (system, store, saga);
    }

    /// <summary>A stop-interrupted in-flight command must not be recorded as a failure.</summary>
    [Test]
    public async Task StopDuringInFlightCommand_DoesNotPersistSagaFailed()
    {
        var (system, store, saga) = await NewSaga();

        var ask = system.AskAsync<object?>(saga.Id, new FixAWaitForStop()).AsTask();
        await saga.Entered.Task;

        await system.StopAsync(saga.Id);

        // The waiting command is interrupted by the stop, not failed by business logic.
        var cancelled = false;
        try
        {
            await ask;
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
        }
        await Assert.That(cancelled).IsTrue();

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Any(e => e is SagaFailed)).IsFalse();
        await Assert.That(saga.IsFailed).IsFalse();
        await Assert.That(saga.IsCompleted).IsFalse();
    }

    /// <summary>
    /// Cancellation from a caller-owned token is still a business failure: the saga
    /// terminates with SagaFailed and the caller sees SagaExecutionException.
    /// </summary>
    [Test]
    public async Task UserTokenCancellation_IsStillABusinessFailure()
    {
        var (system, store, saga) = await NewSaga();

        using var userCts = new CancellationTokenSource();
        var ask = system.AskAsync<object?>(saga.Id, new FixAWaitForUserToken(userCts)).AsTask();
        await saga.Entered.Task;

        await userCts.CancelAsync();

        await Assert.That(async () => await ask).Throws<SagaExecutionException>();

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Any(e => e is SagaFailed)).IsTrue();
        await Assert.That(saga.IsFailed).IsTrue();
        await Assert.That(saga.IsCompleted).IsFalse();
    }

    /// <summary>
    /// MarkComplete then stopped before the flush: the pending completion must be
    /// dropped — no fake SagaCompleted for work that never finished.
    /// </summary>
    [Test]
    public async Task StopAfterMarkComplete_DoesNotPersistFakeCompletion()
    {
        var (system, store, saga) = await NewSaga();

        var ask = system.AskAsync<object?>(saga.Id, new FixACompleteThenWaitForStop()).AsTask();
        await saga.Entered.Task;

        await system.StopAsync(saga.Id);

        try
        {
            await ask;
        }
        catch (OperationCanceledException)
        { /* shutdown interrupt */
        }

        var events = await store.LoadAsync(saga.Id);
        await Assert.That(events.Any(e => e is SagaCompleted)).IsFalse();
        await Assert.That(events.Any(e => e is SagaFailed)).IsFalse();
        await Assert.That(saga.IsCompleted).IsFalse();
        await Assert.That(saga.IsFailed).IsFalse();
    }
}
