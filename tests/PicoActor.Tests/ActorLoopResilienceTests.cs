namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Message-loop resilience: a failure on an error-reporting path (throwing log sink)
// must not kill the consumption loop, and a loop that does die must not leave
// queued Ask callers waiting forever.
// ═══════════════════════════════════════════════════════════

internal sealed record ResilientCreate : ICommand;

internal sealed record ResilientBoom : ICommand;

internal sealed record ResilientProbe : ICommand;

/// <summary>Counts processed probes; ResilientBoom throws, everything else counts.</summary>
internal sealed class ResilientActor : Actor
{
    public int Processed;

    public ResilientActor() { }

    public ResilientActor(ResilientCreate cmd)
        : base(cmd) { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is ResilientBoom)
            throw new InvalidOperationException("business boom");

        // Only probes count — the creation command must not skew the counter
        if (command is ResilientProbe)
            Processed++;

        return new ValueTask<object?>(Processed);
    }
}

internal sealed record InitProbeEvent : IDomainEvent;

internal sealed record InitProbe : ICommand;

/// <summary>
/// Init-recovery actor whose OnReadyAsync blocks on a test-controlled gate and then
/// fails — used to reproduce "init failure with a queued Ask".
/// </summary>
internal sealed class SlowInitActor : EventSourcedActor
{
    public readonly TaskCompletionSource<bool> ReadyEntered = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    public readonly TaskCompletionSource<bool> Release = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public SlowInitActor() { }

    protected override async ValueTask OnReadyAsync()
    {
        ReadyEntered.TrySetResult(true);
        await Release.Task.ConfigureAwait(false);
    }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => new((object?)1);

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>Store that serves a seeded stream (so GetAsync rebuilds) and never appends.</summary>
internal sealed class SeededReadOnlyStore : IEventStore
{
    private readonly IReadOnlyList<IDomainEvent> _events;

    public SeededReadOnlyStore(IReadOnlyList<IDomainEvent> events) => _events = events;

    public ValueTask<ulong> AppendAsync(
        Guid actorId,
        ulong expectedVersion,
        IReadOnlyList<IDomainEvent> events
    ) => throw new NotImplementedException();

    public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) => new(_events);

    public ValueTask<IDomainEvent?> PeekFirstAsync(Guid actorId) => new(_events[0]);
}

public class ActorLoopResilienceTests
{
    /// <summary>
    /// A throwing error handler (the logging seam: ActorSystem routes unhandled Send
    /// errors to ILogger) must not terminate the consumption loop — the actor keeps
    /// processing messages afterwards and later Ask calls complete.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task ThrowingErrorHandler_DoesNotKillLoop_LaterAskCompletes(
        CancellationToken cancellationToken = default
    )
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<ResilientActor>(cmd => new ResilientActor((ResilientCreate)cmd));

        var actor = await system.CreateAsync<ResilientActor>(new ResilientCreate());

        // Simulate an ILogger sink that throws while reporting an unhandled Send error
        actor.UnhandledErrorHandler = (_, _) =>
            throw new InvalidOperationException("logger sink failure");

        system.Send(actor.Id, new ResilientBoom());

        var probe = system.AskAsync<int>(actor.Id, new ResilientProbe()).AsTask();
        var completed = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(3)));

        // Regression: before error-handler isolation, the loop died with the handler's
        // exception and this Ask never completed.
        await Assert.That(ReferenceEquals(completed, probe)).IsTrue();
        await Assert.That(probe.Result).IsEqualTo(1);
    }

    /// <summary>
    /// Initialization failure (OnReadyAsync threw) with an Ask already queued in the
    /// mailbox: the caller must observe a fault, not a permanent hang (the loop exits
    /// before reading the envelope).
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task InitFailure_FaultsQueuedAsk_InsteadOfHanging(
        CancellationToken cancellationToken = default
    )
    {
        var id = Guid.CreateVersion7();
        var store = new SeededReadOnlyStore(new IDomainEvent[] { new InitProbeEvent() });
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        SlowInitActor? created = null;
        system.Register<SlowInitActor>(
            _ => throw new InvalidOperationException(),
            () => created = new SlowInitActor()
        );

        var rebuilding = system.GetAsync<SlowInitActor>(id).AsTask();

        // Deterministic: the actor is registered and gated inside OnReadyAsync
        await created!.ReadyEntered.Task;

        var queued = system.AskAsync<int>(id, new InitProbe()).AsTask();

        // Init fails → the loop exits with the envelope still in the mailbox
        created.Release.TrySetException(new InvalidOperationException("init boom"));

        await Assert.That(async () => await rebuilding).Throws<InvalidOperationException>();

        var completed = await Task.WhenAny(queued, Task.Delay(TimeSpan.FromSeconds(3)));

        // Regression: before FailPendingEnvelopes, the envelope was never read and this
        // Ask hung forever.
        await Assert.That(ReferenceEquals(completed, queued)).IsTrue();
        await Assert.That(queued.IsFaulted).IsTrue();
    }
}
