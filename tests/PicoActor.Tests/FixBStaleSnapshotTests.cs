namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// P2 — the pre-gate prefetch in GetAsync must never be trusted
//      blindly: the shutdown side writes to the same stream while
//      the stopped loop keeps draining (it flushes the terminal
//      event after the registry entry is gone). A caller that
//      rebuilt from the pre-shutdown snapshot would resume a saga
//      that has already completed and die with ConcurrencyException
//      — or, worse, append a second terminal batch.
// ═══════════════════════════════════════════════════════════

internal sealed record FixBStaleCreate : ICommand;

internal sealed record FixBStaleStart : ICommand;

internal sealed record FixBStaleBlock(
    TaskCompletionSource<bool> Entered,
    TaskCompletionSource<bool> Release
) : ICommand;

internal sealed record FixBStaleStarted : IDomainEvent;

internal sealed record FixBStaleResumedStep : IDomainEvent;

/// <summary>
/// Saga used by the stale-snapshot scenario: it can be started (one business event, so the
/// stream is non-empty), parked inside a message turn, and — once released — completed. The
/// resume path deliberately raises an event AND completes, i.e. it appends on top of the
/// version baseline it replayed: a rebuild from a stale snapshot is guaranteed to conflict
/// instead of silently double-resuming.
/// </summary>
internal sealed class FixBStaleSaga : SagaActor
{
    /// <summary>ResumeAsync executions (reset by the test).</summary>
    public static int ResumeCount;

    public FixBStaleSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        command switch
        {
            FixBStaleStart => Start(),
            FixBStaleBlock blocked => ParkAsync(blocked),
            _ => default,
        };

    private ValueTask<object?> Start()
    {
        RaiseEvent(new FixBStaleStarted());
        return new ValueTask<object?>(true);
    }

    private async ValueTask<object?> ParkAsync(FixBStaleBlock blocked)
    {
        blocked.Entered.TrySetResult(true);
        await blocked.Release.Task.ConfigureAwait(false);
        MarkComplete("done");
        return null;
    }

    protected override ValueTask ResumeAsync()
    {
        Interlocked.Increment(ref ResumeCount);
        RaiseEvent(new FixBStaleResumedStep());
        MarkComplete("resumed");
        return default;
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>
/// Store that exposes the exact window the bug lives in. Appends go to a real
/// <see cref="InMemoryEventStore"/> (so version conflicts are the real ones), while the
/// FIRST <see cref="LoadAsync"/> captures the stream as it is right now — before the
/// stopped-but-draining loop flushes its terminal event — and then parks the caller until
/// the test releases it. Every later load reads the current stream.
/// </summary>
internal sealed class FixBStaleSnapshotStore : IEventStore
{
    private readonly InMemoryEventStore _inner = new();
    private readonly TaskCompletionSource<bool> _firstLoadEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource<bool> _releaseFirstLoad = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private int _loads;

    /// <summary>Completes when the pre-gate prefetch is parked with its stale snapshot.</summary>
    public Task FirstLoadEntered => _firstLoadEntered.Task;

    /// <summary>Hand the stale snapshot to the caller that is waiting inside the prefetch.</summary>
    public void ReleaseFirstLoad() => _releaseFirstLoad.TrySetResult(true);

    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        if (Interlocked.Increment(ref _loads) > 1)
            return await _inner.LoadAsync(actorId).ConfigureAwait(false);

        var snapshot = await _inner.LoadAsync(actorId).ConfigureAwait(false);
        _firstLoadEntered.TrySetResult(true);
        await _releaseFirstLoad.Task.ConfigureAwait(false);
        return snapshot;
    }

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => _inner.AppendAsync(actorId, expectedVersion, events);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => _inner.PeekFirstAsync(actorId);
}

public sealed class FixBStaleSnapshotTests
{
    /// <summary>
    /// Deterministic replay of the stale-prefetch window: a saga is parked inside a message
    /// turn, its registry entry is removed by StopAsync while the loop keeps draining,
    /// GetAsync prefetches the (still terminal-free) stream, the released loop flushes
    /// SagaCompleted, and only then does GetAsync continue. It must re-read under the gate,
    /// observe the terminal state and return without rebuilding: no second resume, no
    /// ConcurrencyException, no duplicate terminal batch.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_PrefetchGoesStaleDuringShutdown_DoesNotResumeOnStaleSnapshot(
        CancellationToken cancellationToken = default
    )
    {
        FixBStaleSaga.ResumeCount = 0;
        var store = new FixBStaleSnapshotStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixBStaleSaga>(_ => new FixBStaleSaga(), () => new FixBStaleSaga());

        var saga = await system.CreateAsync<FixBStaleSaga>(new FixBStaleCreate());
        var id = saga.Id;

        // One business event, so the stream is non-empty and GetAsync takes the rebuild path.
        await system.AskAsync<bool>(id, new FixBStaleStart());

        var entered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Send(id, new FixBStaleBlock(entered, release));
        await entered.Task; // the loop is parked inside the handler

        // Shutdown: the registry entry goes away immediately; the parked loop keeps running.
        var stopTask = system.StopAsync(id).AsTask();

        // GetAsync prefetches BEFORE taking the gate: it sees a stream without a terminal
        // event and then parks in the controlled store.
        var getTask = Task.Run(() => system.GetAsync<FixBStaleSaga>(id).AsTask());
        await store.FirstLoadEntered;

        // Release the parked turn: the stopped-but-draining loop completes the saga and
        // flushes SagaCompleted. Waiting for the stop makes the flush observable.
        release.TrySetResult(true);
        await stopTask;

        // Now hand the stale (terminal-free) snapshot to the caller waiting in the prefetch.
        store.ReleaseFirstLoad();

        var rebuilt = await getTask;

        // The saga is terminal: no rebuild is handed out, and nothing resumes on top of it.
        await Assert.That(rebuilt).IsNull();
        await Assert.That(FixBStaleSaga.ResumeCount).IsEqualTo(0);

        // The terminal event is the only completion that reached the stream (no duplicate batch).
        var stream = await store.LoadAsync(id);
        await Assert.That(stream.Count).IsEqualTo(2);
        await Assert.That(stream[1]).IsTypeOf<SagaCompleted>();
    }
}
