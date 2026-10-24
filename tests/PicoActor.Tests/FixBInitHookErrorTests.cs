namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// P5 — OnInitialized() runs OUTSIDE any message turn, so a
//      throwing subclass hook has no caller to surface to. It
//      must be reported through the error seam and it must kill
//      the actor, instead of silently faulting the loop task
//      (a half-initialized actor that looks alive).
// ═══════════════════════════════════════════════════════════

internal sealed class FixBInitHookProbe
{
    /// <summary>Completes from inside the hook, right before it throws.</summary>
    public TaskCompletionSource<bool> HookEntered { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}

/// <summary>Actor whose post-init lifecycle hook throws (a subclass bug).</summary>
internal sealed class FixBThrowingHookActor : ActorBase
{
    private readonly FixBInitHookProbe _probe;

    public FixBThrowingHookActor(FixBInitHookProbe probe) => _probe = probe;

    public bool StopRequested => StopToken.IsCancellationRequested;

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    // internal virtual in Abs; visible here through InternalsVisibleTo.
    internal override void OnInitialized()
    {
        _probe.HookEntered.TrySetResult(true);
        throw new InvalidOperationException("hook boom");
    }
}

public sealed class FixBInitHookErrorTests
{
    /// <summary>
    /// The hook throws after initialization was already published: the actor must be
    /// reported through the error seam and stopped, and the observation is deterministic —
    /// Post stops accepting messages, the stop token is cancelled, and StopAsync observes a
    /// loop that ended normally instead of rethrowing a swallowed hook exception.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task OnInitializedThrows_ReportsErrorAndStopsActor(
        CancellationToken cancellationToken = default
    )
    {
        var probe = new FixBInitHookProbe();
        var reported = new TaskCompletionSource<(Exception Error, ICommand Command)>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        var actor = new FixBThrowingHookActor(probe);
        actor.AttachToSystem(Guid.CreateVersion7());
        actor.UnhandledErrorHandler = (ex, cmd) => reported.TrySetResult((ex, cmd));
        actor.SignalReady();

        await probe.HookEntered.Task;

        // Nothing else observes this exception: without the handler route the loop task
        // just faults (unobserved) and the actor stays "alive" with a dead loop.
        var report = await reported.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
        await Assert.That(report.Error).IsTypeOf<InvalidOperationException>();
        await Assert.That(report.Error.Message).Contains("hook boom");

        // The loop must end NORMALLY (a faulted loop task would surface here) and the actor
        // must be dead, not half-initialized: mailbox closed, stop token cancelled.
        await actor.StopAsync();
        await Assert.That(actor.Post(new Envelope { Command = new FixBCreateCmd() })).IsFalse();
        await Assert.That(actor.StopRequested).IsTrue();
    }
}
