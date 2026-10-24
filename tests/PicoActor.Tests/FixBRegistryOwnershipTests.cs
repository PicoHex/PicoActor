namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// B1 — an init-failure must clean up THE instance that failed,
//      never a same-id replacement registered by another thread.
// ═══════════════════════════════════════════════════════════

internal sealed record FixBCreate(int Value) : ICommand;

internal sealed record FixBGetValue : ICommand;

internal sealed record FixBCreated(int Value) : IDomainEvent;

/// <summary>
/// Event-sourced actor used by the B1 scenario. The creation path stages FixBCreated in
/// the constructor, so <c>OnReadyAsync</c> → FlushEventsAsync is the first thing that
/// touches the store — the seam this test blocks.
/// </summary>
internal sealed class FixBOwnershipActor : EventSourcedActor
{
    private int _value;

    public FixBOwnershipActor(FixBCreate cmd)
        : base(cmd) { }

    public FixBOwnershipActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        command switch
        {
            FixBCreate c => RaiseCreate(c.Value),
            FixBGetValue => new ValueTask<object?>(_value),
            _ => default,
        };

    private ValueTask<object?> RaiseCreate(int value)
    {
        RaiseEvent(new FixBCreated(value));
        return default;
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is FixBCreated e)
            _value = e.Value;
    }
}

/// <summary>
/// Store whose FIRST AppendAsync blocks until <see cref="Release"/> and then throws
/// (an infrastructure failure during initialization). Later appends succeed — the
/// concurrent rebuild must be able to initialize.
/// </summary>
internal sealed class FixBBlockingAppendStore : IEventStore
{
    private readonly IReadOnlyList<IDomainEvent> _stream;
    private readonly TaskCompletionSource<bool> _appendEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private readonly TaskCompletionSource<bool> _release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private int _appendCount;

    public FixBBlockingAppendStore(IReadOnlyList<IDomainEvent> stream) => _stream = stream;

    /// <summary>Completes when the blocked append is entered (so the actor id is known).</summary>
    public Task AppendEntered => _appendEntered.Task;

    /// <summary>Actor id the blocked (first) append belongs to.</summary>
    public Guid BlockedActorId { get; private set; }

    public void Release() => _release.TrySetResult(true);

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    )
    {
        if (Interlocked.Increment(ref _appendCount) > 1)
            return new ValueTask<ulong>(expectedVersion + (ulong)events.Count);

        BlockedActorId = actorId;
        _appendEntered.TrySetResult(true);
        return BlockThenFailAsync();
    }

    private async ValueTask<ulong> BlockThenFailAsync()
    {
        await _release.Task.ConfigureAwait(false);
        throw new IOException("append failed after release");
    }

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) => new(_stream);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) =>
        new(_stream.Count > 0 ? _stream[0] : null);
}

public sealed class FixBRegistryOwnershipTests
{
    /// <summary>
    /// A's initialization fails while A is no longer the registry owner of its id (it was
    /// stopped, then a concurrent GetAsync rebuilt B from the same stream). The failure
    /// cleanup must remove A only if A is still the registered instance — removing
    /// blindly by key deletes B's live registration.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task InitFailure_AfterConcurrentRebuild_DoesNotRemoveTheRebuiltInstance(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBLogSink();
        var store = new FixBBlockingAppendStore(new IDomainEvent[] { new FixBCreated(7) });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store, Logger = log });

        system.Register<FixBOwnershipActor>(
            cmd =>
                cmd is FixBCreate c
                    ? new FixBOwnershipActor(c)
                    : throw new InvalidOperationException("unexpected command"),
            () => new FixBOwnershipActor()
        );

        // A: creation path, stuck inside init on the blocked append (already registered).
        var createTask = Task.Run(() =>
            system.CreateAsync<FixBOwnershipActor>(new FixBCreate(1)).AsTask()
        );
        await store.AppendEntered;
        var id = store.BlockedActorId;

        // Stop A while it sits in init. StopAsync removes the registry entry synchronously
        // (before it awaits the loop), so the removal is deterministic: the loop itself
        // cannot finish until the append is released.
        var stopTask = system.StopAsync(id).AsTask();
        await Assert.That(() => system.Send(id, new FixBGetValue())).Throws<KeyNotFoundException>();

        // Concurrent rebuild: B is constructed from the same stream, registered under the
        // SAME id, and initializes successfully.
        var rebuilt = await system.GetAsync<FixBOwnershipActor>(id);
        await Assert.That(rebuilt).IsNotNull();
        var b = rebuilt!;
        await Assert.That(b.Id).IsEqualTo(id);
        await Assert.That(await system.AskAsync<int>(id, new FixBGetValue())).IsEqualTo(7);

        // Release A → its init fails → the failure cleanup runs.
        store.Release();
        await log.InitFailureLogged;

        // Cleanup must not have deleted B: still registered, still reachable, still B.
        await Assert.That(await system.AskAsync<int>(id, new FixBGetValue())).IsEqualTo(7);
        await Assert
            .That(ReferenceEquals(await system.GetAsync<FixBOwnershipActor>(id), b))
            .IsTrue();

        try
        {
            await stopTask; // A's loop faults with the init exception
            await createTask; // A's CreateAsync rethrows it
        }
        catch (IOException)
        { /* expected: A's initialization failed */
        }
    }
}
