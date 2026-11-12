namespace PicoActor.Abs.Tests;

// ═══════════════════════════════════════════════════════════
// N-1 / N-3 — the two SELF-STOP routes out of Actor are
//      OWNERSHIP-CHECKED. A queued passivation sentinel, and a
//      throwing OnInitialized hook, must ask the host to stop
//      THIS instance (TryRequestStop(id, this)); the id-only
//      System.RequestStop(Id) is only the fallback for a host
//      that does not implement IActorPassivationHost.
//
//      The distinction is not cosmetic: an id-only removal can
//      unregister a REBUILT replacement that owns the id by the
//      time the sentinel is consumed, while the instance that
//      asked to leave stays registered. Nothing else in the suite
//      fails if the ownership check is removed — that is why
//      these probes exist.
// ═══════════════════════════════════════════════════════════

/// <summary>
/// Recording <see cref="IActorSystem"/>: the only thing this probe cares about is the ID-ONLY
/// stop route (<see cref="RequestStop"/>). Everything else is out of scope.
/// </summary>
internal class RecordingStopSystem : IActorSystem
{
    private int _idOnlyStopCount;

    /// <summary>Completes with the id of the FIRST id-only stop request.</summary>
    public TaskCompletionSource<Guid> IdOnlyStop { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>How many id-only stops were requested (read after the loop exited).</summary>
    public int IdOnlyStopCount => Volatile.Read(ref _idOnlyStopCount);

    public void RequestStop(Guid id)
    {
        Interlocked.Increment(ref _idOnlyStopCount);
        IdOnlyStop.TrySetResult(id);
    }

    public ValueTask StopAsync(Guid id) => default;

    public void Register<T>(Func<ICommand, T> createFactory, Func<T>? rebuildFactory = null)
        where T : IActor => throw new NotSupportedException();

    public ValueTask<T> CreateAsync<T>(ICommand command)
        where T : IActor => throw new NotSupportedException();

    public ValueTask<T?> GetAsync<T>(Guid id)
        where T : IActor => throw new NotSupportedException();

    public void Send(Guid id, ICommand command) => throw new NotSupportedException();

    public ValueTask<TResult> AskAsync<TResult>(Guid id, ICommand command) =>
        throw new NotSupportedException();

    public ValueTask StopAllAsync() => throw new NotSupportedException();

    public ValueTask<IReadOnlyList<Guid>> FindAggregateIds(
        string firstEventType,
        Func<IDomainEvent, bool> firstEventMatch
    ) => throw new NotSupportedException();

    public ValueTask<IReadOnlyList<SagaResumeResult>> ResumeInterruptedSagasAsync<TSaga>(
        string firstEventType,
        Func<IDomainEvent, bool>? firstEventMatch = null,
        Action<Exception, Guid>? onItemError = null
    )
        where TSaga : SagaActor => throw new NotSupportedException();

    public ValueTask<SagaExecution<TResult>> ExecuteSaga<TSaga, TResult>(ICommand command)
        where TSaga : SagaActor => throw new NotSupportedException();
}

/// <summary>
/// The same recorder, plus the passivation host half: a runtime that CAN answer "is this
/// instance still the one registered for that id?".
/// <para>
/// <see cref="TryRequestStop"/> returns true (the instance was still the registered one) so the
/// assertion is about the ROUTE taken, not about the host's verdict on it.
/// </para>
/// </summary>
internal sealed class RecordingPassivationHost : RecordingStopSystem, IActorPassivationHost
{
    /// <summary>Completes with the first ownership-checked stop request.</summary>
    public TaskCompletionSource<(Guid Id, IActor Expected)> OwnStop { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public bool TryRequestStop(Guid id, IActor expected)
    {
        OwnStop.TrySetResult((id, expected));
        return true;
    }
}

/// <summary>Does nothing: neither route dispatches a command to the subclass.</summary>
internal sealed class SentinelNoOpActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;
}

/// <summary>Actor whose post-initialization hook throws (a subclass bug).</summary>
internal sealed class ThrowingHookActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    // internal virtual in Abs; visible here through InternalsVisibleTo.
    internal override void OnInitialized() => throw new InvalidOperationException("hook boom");
}

public sealed class PassivationSentinelRoutingTests
{
    /// <summary>
    /// The passivation sentinel must stop THIS instance through
    /// <see cref="IActorPassivationHost.TryRequestStop"/>. Reverting that call to the old
    /// id-only <c>System?.RequestStop(Id)</c> leaves every other test green — this is the
    /// test that goes red.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task PassivationSentinel_StopsThisInstance_ThroughTheOwnershipCheckedHost(
        CancellationToken cancellationToken = default
    )
    {
        var host = new RecordingPassivationHost();
        var actor = new SentinelNoOpActor();
        var id = Guid.CreateVersion7();
        actor.AttachToSystem(id, host);
        actor.SignalReady();

        await Assert.That(actor.RequestPassivation()).IsTrue();

        var (stoppedId, expected) = await host.OwnStop.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            cancellationToken
        );
        await Assert.That(stoppedId).IsEqualTo(id);
        await Assert.That(ReferenceEquals(expected, actor)).IsTrue();

        // Deterministic "and never the id-only route": the loop has fully exited by the time
        // StopAsync returns, so any stop request the sentinel path was going to make has
        // already been made. No sleep, no grace window.
        await actor.StopAsync();
        await Assert.That(host.IdOnlyStopCount).IsEqualTo(0);
    }

    /// <summary>
    /// A host that does not implement <see cref="IActorPassivationHost"/> keeps the documented
    /// id-only fallback: the sentinel still stops the actor instead of doing nothing.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task PassivationSentinel_FallsBackToTheIdOnlyStop_WhenTheHostHasNoOwnershipCheck(
        CancellationToken cancellationToken = default
    )
    {
        var system = new RecordingStopSystem();
        var actor = new SentinelNoOpActor();
        var id = Guid.CreateVersion7();
        actor.AttachToSystem(id, system);
        actor.SignalReady();

        await Assert.That(actor.RequestPassivation()).IsTrue();

        var stoppedId = await system.IdOnlyStop.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            cancellationToken
        );
        await Assert.That(stoppedId).IsEqualTo(id);

        await actor.StopAsync();
    }

    /// <summary>
    /// N-3: the throwing <see cref="Actor.OnInitialized"/> path is a self-stop too, so it takes
    /// the same ownership-checked route — not "whatever owns my id".
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task ThrowingInitializedHook_StopsThisInstance_ThroughTheOwnershipCheckedHost(
        CancellationToken cancellationToken = default
    )
    {
        var host = new RecordingPassivationHost();
        var actor = new ThrowingHookActor
        {
            // The failure report is already covered by FixB; keep
            // this probe's output on the assertion, not on stderr.
            UnhandledErrorHandler = static (_, _) => { },
        };
        var id = Guid.CreateVersion7();
        actor.AttachToSystem(id, host);
        actor.SignalReady();

        var (stoppedId, expected) = await host.OwnStop.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            cancellationToken
        );
        await Assert.That(stoppedId).IsEqualTo(id);
        await Assert.That(ReferenceEquals(expected, actor)).IsTrue();

        // The hook failure ends the loop normally (it does not fault it), so this is the
        // "loop is gone" signal, and therefore the final word on which route was taken.
        await actor.StopAsync();
        await Assert.That(host.IdOnlyStopCount).IsEqualTo(0);
    }
}
