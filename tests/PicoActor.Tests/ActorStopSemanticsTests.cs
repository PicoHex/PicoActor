namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Stop semantics (characterization): StopAsync/RequestStop remove the actor from the
// registry first, but buffered mailbox messages are still dispatched before the loop
// exits — the queue is DRAINED, not discarded. This is what makes the SagaActor
// terminal-state guard load-bearing, so the behavior is pinned here.
// ═══════════════════════════════════════════════════════════

internal sealed record StopSemanticsCreate : ICommand;

internal sealed record StopSemanticsBlock : ICommand;

internal sealed record StopSemanticsCounted : ICommand;

internal sealed class StopSemanticsActor : Actor
{
    private readonly TaskCompletionSource<bool> _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public readonly TaskCompletionSource<bool> BlockEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public int Counted;

    public StopSemanticsActor() { }

    public StopSemanticsActor(StopSemanticsCreate cmd)
        : base(cmd) { }

    public void ReleaseBlock() => _release.TrySetResult(true);

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is StopSemanticsBlock)
        {
            BlockEntered.TrySetResult(true);
            await _release.Task.ConfigureAwait(false);
        }

        if (command is StopSemanticsCounted)
            Counted++;

        return new ValueTask<object?>(Counted);
    }
}

public sealed class ActorStopSemanticsTests
{
    /// <summary>
    /// Messages buffered when the stop is signalled are processed before the loop exits:
    /// Ask callers that already got their envelope in never hang, and no message is
    /// silently dropped mid-flight.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task StopAsync_DrainsBufferedMessages_BeforeLoopExits(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<StopSemanticsActor>(cmd => new StopSemanticsActor(
            (StopSemanticsCreate)cmd
        ));

        var actor = await system.CreateAsync<StopSemanticsActor>(new StopSemanticsCreate());
        actor.Counted = 0;

        // Occupy the loop, then buffer three more messages behind it
        _ = system.AskAsync<int>(actor.Id, new StopSemanticsBlock()).AsTask();
        await actor.BlockEntered.Task;
        system.Send(actor.Id, new StopSemanticsCounted());
        system.Send(actor.Id, new StopSemanticsCounted());
        system.Send(actor.Id, new StopSemanticsCounted());

        // Stop while the buffered messages are still queued, then let the turn finish
        var stopping = system.StopAsync(actor.Id).AsTask();
        actor.ReleaseBlock();

        // The loop task completion proves the drain finished (no timing assumptions)
        await stopping;

        await Assert.That(actor.Counted).IsEqualTo(3);

        // Registry removal is immediate: later messages fail loudly instead of queueing
        await Assert
            .That(() => system.Send(actor.Id, new StopSemanticsCounted()))
            .Throws<KeyNotFoundException>();
    }
}
