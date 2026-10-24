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

/// <summary>
/// Probe for the REGISTERED variant: the hook blocks until the test releases it, so the
/// creation call has already returned when the hook fails (no create/stop race).
/// </summary>
internal sealed class FixBBlockedHookProbe
{
    /// <summary>Completes from inside the hook, right before it blocks.</summary>
    public TaskCompletionSource<bool> HookEntered { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Releases the hook so it proceeds to throw.</summary>
    public ManualResetEventSlim Release { get; } = new(false);
}

/// <summary>
/// Actor whose post-init hook fails only once the test lets it: the failure therefore
/// happens on an actor that CreateAsync has already returned to the caller.
/// </summary>
internal sealed class FixBLateThrowingHookActor : ActorBase
{
    private readonly FixBBlockedHookProbe _probe;

    public FixBLateThrowingHookActor(FixBBlockedHookProbe probe) => _probe = probe;

    public bool StopRequested => StopToken.IsCancellationRequested;

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    internal override void OnInitialized()
    {
        _probe.HookEntered.TrySetResult(true);
        // The hook is synchronous by contract (internal virtual void): gate the loop
        // thread until the test confirms the actor was handed out.
        _probe.Release.Wait(TimeSpan.FromSeconds(10));
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

    /// <summary>
    /// The same hook failure on an actor that WAS registered through the system. Stopping
    /// the loop alone left the dead instance in the registry: Send/Ask then failed with
    /// InvalidOperationException ("stopping") and GetAsync handed out a dead handle.
    /// A failed actor must leave the registry, so every lookup misses instead.
    /// </summary>
    [Test]
    [Timeout(30000)]
    public async Task OnInitializedThrows_RegisteredActor_LeavesTheRegistry(
        CancellationToken cancellationToken = default
    )
    {
        var probe = new FixBBlockedHookProbe();
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<FixBLateThrowingHookActor>(_ => new FixBLateThrowingHookActor(probe));

        var actor = await system.CreateAsync<FixBLateThrowingHookActor>(new FixBCreateCmd());
        await probe.HookEntered.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);

        // Release the hook and wait until the stop reached the actor. The runtime removes
        // the registry entry BEFORE signalling the stop, so observing the stop means the
        // registry is already clean (no sleep-and-hope).
        probe.Release.Set();
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!actor.StopRequested && DateTime.UtcNow < deadline)
            await Task.Delay(5);
        await Assert.That(actor.StopRequested).IsTrue();

        await Assert
            .That(() => system.Send(actor.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
        await Assert
            .That(async () => await system.AskAsync<object?>(actor.Id, new FixBCreateCmd()))
            .Throws<KeyNotFoundException>();
        await Assert.That(await system.GetAsync<FixBLateThrowingHookActor>(actor.Id)).IsNull();
    }
}
