namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// B2 — GetAsync is single-flight per id, and a registry hit
//      never hands out an instance whose init is still running.
// ═══════════════════════════════════════════════════════════

/// <summary>
/// Store whose LoadAsync parks every caller until <see cref="Release"/>, and counts the
/// loads. Lets a test keep callers parked between the registry check and the rebuild.
/// </summary>
internal sealed class FixBSingleFlightStore : IEventStore
{
    private readonly IReadOnlyList<IDomainEvent> _stream;
    private readonly TaskCompletionSource<bool> _loadEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource<bool> _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _loadCount;

    public FixBSingleFlightStore(IReadOnlyList<IDomainEvent> stream) => _stream = stream;

    public Task LoadEntered => _loadEntered.Task;

    public int LoadCount => Volatile.Read(ref _loadCount);

    public void Release() => _release.TrySetResult(true);

    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        Interlocked.Increment(ref _loadCount);
        _loadEntered.TrySetResult(true);
        await _release.Task.ConfigureAwait(false);
        return _stream;
    }

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => new(expectedVersion + (ulong)events.Count);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
        new(_stream.Count > 0 ? _stream[0] : null);
}

/// <summary>
/// Event-sourced actor whose initialization parks on the supplied gate, so a test can
/// hold an instance in "registered but not initialized" state.
/// </summary>
internal sealed class FixBBlockingReadyActor : EventSourcedActor
{
    private readonly TaskCompletionSource<bool> _initEntered;
    private readonly TaskCompletionSource<bool> _releaseInit;

    public FixBBlockingReadyActor(
        TaskCompletionSource<bool> initEntered,
        TaskCompletionSource<bool> releaseInit
    )
    {
        _initEntered = initEntered;
        _releaseInit = releaseInit;
    }

    /// <summary>True once initialization ran to completion.</summary>
    public bool Ready { get; private set; }

    protected override async ValueTask OnReadyAsync()
    {
        _initEntered.TrySetResult(true);
        await _releaseInit.Task.ConfigureAwait(false);
        await base.OnReadyAsync().ConfigureAwait(false);
        Ready = true;
    }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        command is FixBGetValue ? new ValueTask<object?>(42) : default;

    protected override void Mutate(IDomainEvent @event) { }
}

// ═══════════════════════════════════════════════════════════
// B2 (2) — a stale loser must not resume a saga that the
//          winner already completed and unregistered.
// ═══════════════════════════════════════════════════════════

/// <summary>Counts saga resumes across the instances a test builds (shared, not static).</summary>
internal sealed class FixBSagaProbe
{
    public int Resumes;
}

/// <summary>
/// Saga whose recovery resume parks until the test releases it, then completes. Lets a test
/// hold the winner's initialization open long enough for a second caller to hold a stale
/// event snapshot.
/// </summary>
internal sealed class FixBResumingSaga : SagaActor
{
    private readonly FixBSagaProbe _probe;
    private readonly TaskCompletionSource<bool> _resumeEntered;
    private readonly TaskCompletionSource<bool> _releaseResume;

    public FixBResumingSaga(
        FixBSagaProbe probe,
        TaskCompletionSource<bool> resumeEntered,
        TaskCompletionSource<bool> releaseResume
    )
    {
        _probe = probe;
        _resumeEntered = resumeEntered;
        _releaseResume = releaseResume;
    }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override async ValueTask ResumeAsync()
    {
        Interlocked.Increment(ref _probe.Resumes);
        _resumeEntered.TrySetResult(true);
        await _releaseResume.Task.ConfigureAwait(false);
        MarkComplete("resumed");
    }

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>
/// Version-checking in-memory stream whose first two LoadAsync calls park until
/// <see cref="Release"/> (so two callers can hold the same pre-flush snapshot), and whose
/// later calls return the advanced stream.
/// </summary>
internal sealed class FixBSagaRaceStore : IEventStore
{
    private readonly List<IDomainEvent> _stream = [];
    private readonly Lock _sync = new();
    private readonly TaskCompletionSource<bool> _loadEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource<bool> _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _loadCount;

    public FixBSagaRaceStore(IDomainEvent first) => _stream.Add(first);

    public Task LoadEntered => _loadEntered.Task;

    public void Release() => _release.TrySetResult(true);

    public async ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
    {
        if (Interlocked.Increment(ref _loadCount) <= 2)
        {
            _loadEntered.TrySetResult(true);
            await _release.Task.ConfigureAwait(false);
        }

        lock (_sync)
            return _stream.ToList();
    }

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        lock (_sync)
        {
            if ((ulong)_stream.Count != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, (ulong)_stream.Count);

            _stream.AddRange(events);
            return new ValueTask<ulong>((ulong)_stream.Count);
        }
    }

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId)
    {
        lock (_sync)
            return new ValueTask<IDomainEvent?>(_stream.Count > 0 ? _stream[0] : null);
    }
}

public sealed class FixBGetAsyncSingleFlightTests
{
    /// <summary>
    /// Two callers miss the registry for the same id. Only one rebuild may run: the
    /// second caller waits for the first registration instead of replaying the stream
    /// again (a duplicate rebuild replays twice and, for a saga, resumes twice).
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_ConcurrentRegistryMiss_RebuildsOnce(
        CancellationToken cancellationToken = default
    )
    {
        var store = new FixBSingleFlightStore(new IDomainEvent[] { new FixBCreated(3) });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        var builds = 0;
        system.Register<FixBOwnershipActor>(
            _ => throw new InvalidOperationException("no creation path in this test"),
            () =>
            {
                Interlocked.Increment(ref builds);
                return new FixBOwnershipActor();
            }
        );

        var id = Guid.CreateVersion7();

        // Caller #1 parks inside LoadAsync, so nothing is registered yet.
        var first = Task.Run(() => system.GetAsync<FixBOwnershipActor>(id).AsTask());
        await store.LoadEntered;

        // Caller #2 is invoked synchronously: by the time the ValueTask comes back it has
        // provably already passed the (still empty) registry check, so it cannot be
        // classified as a registry hit — it must park instead of rebuilding.
        var second = system.GetAsync<FixBOwnershipActor>(id);
        await Assert.That(second.IsCompleted).IsFalse();

        store.Release();

        var a = await first;
        var b = await second;

        // Single rebuild: the second caller waited for the first registration instead of
        // replaying the stream into a second copy of the aggregate (the stream itself may
        // be read more than once - reads have no side effects, builds/replays/resumes do).
        await Assert.That(builds).IsEqualTo(1);
        await Assert.That(a).IsNotNull();
        await Assert.That(ReferenceEquals(a, b)).IsTrue();
        await Assert.That(await system.AskAsync<int>(id, new FixBGetValue())).IsEqualTo(3);
    }

    /// <summary>
    /// A registry hit on an instance whose initialization is still running must wait for
    /// it: returning early hands out an actor that can still remove itself (terminal
    /// saga) or be removed by its own init failure.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_RegistryHitWhileInitializing_WaitsForInitialization(
        CancellationToken cancellationToken = default
    )
    {
        var store = new FixBSingleFlightStore(new IDomainEvent[] { new FixBCreated(0) });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        var initEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseInit = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Register<FixBBlockingReadyActor>(
            _ => throw new InvalidOperationException("no creation path in this test"),
            () => new FixBBlockingReadyActor(initEntered, releaseInit)
        );

        var id = Guid.CreateVersion7();
        store.Release(); // the load itself is not what this test holds

        var first = Task.Run(() => system.GetAsync<FixBBlockingReadyActor>(id).AsTask());
        await initEntered.Task; // registered, initialization in flight

        var second = system.GetAsync<FixBBlockingReadyActor>(id);
        await Assert.That(second.IsCompleted).IsFalse();

        releaseInit.TrySetResult(true);

        var a = await first;
        var b = await second;

        await Assert.That(b).IsNotNull();
        await Assert.That(ReferenceEquals(a, b)).IsTrue();
        await Assert.That(b!.Ready).IsTrue();
    }

    /// <summary>
    /// The rebuilt instance can lose its registration while it initializes (a concurrent
    /// stop). It is then dead: GetAsync must report "not found" rather than hand out a
    /// non-terminal stopped actor.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_RegistrationLostDuringInit_ReturnsDefault(
        CancellationToken cancellationToken = default
    )
    {
        var store = new FixBSingleFlightStore(new IDomainEvent[] { new FixBCreated(0) });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        var initEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseInit = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Register<FixBBlockingReadyActor>(
            _ => throw new InvalidOperationException("no creation path in this test"),
            () => new FixBBlockingReadyActor(initEntered, releaseInit)
        );

        var id = Guid.CreateVersion7();
        store.Release();

        var first = Task.Run(() => system.GetAsync<FixBBlockingReadyActor>(id).AsTask());
        await initEntered.Task;

        // Stop the initializing instance: the registry entry disappears synchronously,
        // while its initialization still has to finish.
        var stopTask = system.StopAsync(id).AsTask();
        await Assert.That(() => system.Send(id, new FixBGetValue())).Throws<KeyNotFoundException>();

        releaseInit.TrySetResult(true);

        await Assert.That(await first).IsNull();
        await stopTask;
    }
    /// <summary>
    /// The core duplicate-resume race: two callers hold the same pre-flush snapshot; the
    /// winner rebuilds, resumes and completes the saga, which unregisters itself during
    /// initialization. The loser must notice that its snapshot is stale and re-read the
    /// stream (finding the terminal event) instead of registering a second copy and
    /// resuming it again - the second resume would flush on top of the first and die with
    /// ConcurrencyException.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_ConcurrentRebuild_TerminalSagaResumesOnce(
        CancellationToken cancellationToken = default
    )
    {
        var store = new FixBSagaRaceStore(new SagaStep1Started("x"));
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        var probe = new FixBSagaProbe();
        var resumeEntered = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var releaseResume = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        system.Register<FixBResumingSaga>(
            _ => throw new InvalidOperationException("sagas are rebuilt, never created"),
            () => new FixBResumingSaga(probe, resumeEntered, releaseResume)
        );

        var id = Guid.CreateVersion7();

        // Caller #1 holds the pre-flush snapshot (parked inside LoadAsync).
        var first = Task.Run(() => system.GetAsync<FixBResumingSaga>(id).AsTask());
        await store.LoadEntered;

        // Caller #2 is invoked synchronously: it has provably passed the still-empty
        // registry check, and parks on the same pre-flush snapshot.
        var second = system.GetAsync<FixBResumingSaga>(id);
        await Assert.That(second.IsCompleted).IsFalse();

        store.Release();

        // The winner resumes and unregisters itself; the loser must see the terminal event.
        await resumeEntered.Task;
        releaseResume.TrySetResult(true);

        var a = await first;
        var b = await second;

        // Exactly one resume, even though both callers held the same stale snapshot.
        await Assert.That(Volatile.Read(ref probe.Resumes)).IsEqualTo(1);

        // One caller gets the terminal saga instance (readable, unregistered by design);
        // the other resolves to "not found" - never to a second live saga.
        await Assert.That((a is null) != (b is null)).IsTrue();
        var terminal = a ?? b;
        await Assert.That(terminal).IsNotNull();
        await Assert.That(terminal!.IsCompleted || terminal.IsFailed).IsTrue();
    }
}
