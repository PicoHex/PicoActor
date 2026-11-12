namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// P — idle actor passivation. A resident event-sourced actor's materialized
//     state is only a memory cache: the event stream is the truth. With
//     ActorSystemOptions.IdleTimeout configured (opt-in, default OFF), an actor
//     that has been idle long enough gives its state back, and GetAsync rebuilds
//     it from the stream on the next use.
//     Two properties this file pins:
//       1. Only REBUILDABLE event-sourced actors are candidates — a non-ES actor
//          (or an ES type without a rebuildFactory) would be lost forever.
//       2. The stop goes through a MAILBOX SENTINEL, i.e. a message boundary —
//          never an external SignalStop racing a dispatch. An actor that is
//          processing, or that has a backlog, is not a candidate at all.
//     All waits are poll + cap (or a TCS gate); no fixed sleep guesses at timing.
// ═══════════════════════════════════════════════════════════

internal sealed record FixPCreate : ICommand;

internal sealed record FixPIncrement(int Delta) : ICommand;

internal sealed record FixPGetValue : ICommand;

internal sealed record FixPNoOp : ICommand;

internal sealed record FixPCreated : IDomainEvent;

internal sealed record FixPIncremented(int Delta) : IDomainEvent;

/// <summary>
/// Parks the actor's turn until the test releases it: models a handler that is
/// still doing work (the sweep must treat the actor as busy, not idle).
/// </summary>
internal sealed record FixPPark(
    TaskCompletionSource<bool> Entered,
    TaskCompletionSource<bool> Release
) : ICommand;

/// <summary>Rebuildable event-sourced counter with a parking command.</summary>
internal sealed class FixPCounter : EventSourcedActor
{
    private int _value;

    public FixPCounter(FixPCreate cmd)
        : base(cmd) { }

    public FixPCounter() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixPCreate:
                RaiseEvent(new FixPCreated());
                return null;
            case FixPIncrement increment:
                RaiseEvent(new FixPIncremented(increment.Delta));
                return null;
            case FixPGetValue:
                return _value;
            case FixPPark park:
                park.Entered.TrySetResult(true);
                await park.Release.Task.ConfigureAwait(false);
                return null;
            default:
                return null;
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is FixPIncremented incremented)
            _value += incremented.Delta;
    }
}

/// <summary>
/// A saga that is WAITING for an external reply: it never completes on its own, and its
/// resume is a no-op, so a rebuild just waits again. Waiting IS its normal state, which is
/// exactly why the idle clock cannot be the only question asked of it — passivating it would
/// drop the reply the outside world is about to Send.
/// </summary>
internal sealed record FixPSagaStart(string Name) : ICommand;

internal sealed record FixPSagaReply(string Value) : ICommand;

internal sealed record FixPSagaName : ICommand;

internal sealed record FixPSagaStarted(string Name) : IDomainEvent;

internal sealed class FixPWaitingSaga : SagaActor
{
    private string _name = "";

    public FixPWaitingSaga() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixPSagaStart start:
                RaiseEvent(new FixPSagaStarted(start.Name));
                return default;
            case FixPSagaReply reply:
                return new ValueTask<object?>(reply.Value);
            case FixPSagaName:
                return new ValueTask<object?>(_name);
            default:
                return default;
        }
    }

    /// <summary>Still waiting for the outside world: recovery has nothing to advance.</summary>
    protected override ValueTask ResumeAsync() => default;

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is FixPSagaStarted started)
            _name = started.Name;
    }
}

/// <summary>Non-ES actor: its state exists only in memory, so it can never be reclaimed.</summary>
internal sealed class FixPPlainActor : Actor
{
    public FixPPlainActor(FixPNoOp cmd)
        : base(cmd) { }

    public FixPPlainActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;
}

public sealed class FixPPassivationTests
{
    private const int PollLimitMs = 5_000;

    /// <summary>
    /// Build a system that will not hang a FAILING test: a parked handler is abandoned
    /// after five seconds instead of parking the shutdown (every test stops its system).
    /// </summary>
    private static ActorSystem NewSystem(
        InMemoryEventStore store,
        TimeSpan? idleTimeout,
        TimeSpan? sweepInterval
    ) =>
        new(
            new ActorSystemOptions
            {
                EventStore = store,
                IdleTimeout = idleTimeout,
                IdleSweepInterval = sweepInterval,
                StopTimeout = TimeSpan.FromSeconds(5),
            }
        );

    private static void RegisterCounter(ActorSystem system) =>
        system.Register<FixPCounter>(
            cmd =>
                cmd is FixPCreate create
                    ? new FixPCounter(create)
                    : throw new InvalidOperationException($"unexpected {cmd.GetType().Name}"),
            () => new FixPCounter()
        );

    /// <summary>Poll <paramref name="condition"/> until it holds or the cap expires.</summary>
    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int limitMs = PollLimitMs)
    {
        var deadline = Environment.TickCount64 + limitMs;
        while (Environment.TickCount64 < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(10).ConfigureAwait(false);
        }

        return condition();
    }

    /// <summary>
    /// Drive the (opt-in) sweep by hand until <paramref name="condition"/> holds.
    /// Explicit sweeps make the tests deterministic: no background timer decides when.
    /// </summary>
    private static async Task<bool> SweepUntilAsync(
        ActorSystem system,
        Func<bool> condition,
        int limitMs = PollLimitMs
    )
    {
        var deadline = Environment.TickCount64 + limitMs;
        while (Environment.TickCount64 < deadline)
        {
            await system.SweepIdleActorsAsync().ConfigureAwait(false);
            if (condition())
                return true;
            await Task.Delay(10).ConfigureAwait(false);
        }

        return condition();
    }

    /// <summary>
    /// Wait until the sweep has issued a passivation request past <paramref name="baseline"/>.
    /// The counter is the only SIDE-EFFECT-FREE signal available to a test: an actor's idle clock
    /// is reset by any accepted command, so a Send-based probe would keep the actor alive forever.
    /// </summary>
    private static Task<bool> WaitForPassivationAsync(
        ActorSystem system,
        long baseline,
        int limitMs = PollLimitMs
    ) => WaitUntilAsync(() => system.PassivatedCount > baseline, limitMs);

    /// <summary>
    /// "This id is no longer materialized", observed the way application code sees it:
    /// Send throws <see cref="KeyNotFoundException"/>. <see cref="InvalidOperationException"/>
    /// is the short stop-race window (mailbox closed, registry entry not yet removed) — retry
    /// instead of concluding. Note this probe POSTS a command, so it resets the actor's idle
    /// clock: only use it against an actor that was already passivated.
    /// </summary>
    private static bool IsGone(ActorSystem system, Guid id)
    {
        try
        {
            system.Send(id, new FixPNoOp());
            return false;
        }
        catch (KeyNotFoundException)
        {
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>
    /// P1 + P3 + P4: an idle, rebuildable ES actor is reclaimed within the idle budget,
    /// the registry miss is observable exactly as before (KeyNotFoundException), and
    /// GetAsync rebuilds it — same id, same state, still taking commands.
    /// </summary>
    [Test]
    public async Task IdleEventSourcedActor_IsPassivated_ThenRebuiltByGetAsync()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(
            store,
            TimeSpan.FromMilliseconds(200),
            TimeSpan.FromMilliseconds(25)
        );
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            // Ask is the synchronizing barrier: the increment is applied before it answers.
            system.Send(id, new FixPIncrement(5));
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(5);

            // Background sweeper (IdleTimeout=200ms, tick=25ms): 3s is a generous cap.
            var passivated = await WaitForPassivationAsync(system, 0, 3_000);
            await Assert.That(passivated).IsTrue();

            // The request is a sentinel: the loop consumes it and the entry disappears.
            var gone = await WaitUntilAsync(() => IsGone(system, id), 3_000);
            await Assert.That(gone).IsTrue();

            await Assert
                .That(() => system.Send(id, new FixPIncrement(1)))
                .Throws<KeyNotFoundException>();

            // Explicit activation: the stream is the truth, so the state comes back.
            var rebuilt = await system.GetAsync<FixPCounter>(id);
            await Assert.That(rebuilt).IsNotNull();
            await Assert.That(rebuilt!.Id).IsEqualTo(id);
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(5);

            system.Send(id, new FixPIncrement(2));
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(7);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P3: a non-ES actor has no event stream to rebuild from — it must NEVER be a
    /// candidate, no matter how long it idles (its state would be lost forever).
    /// </summary>
    [Test]
    public async Task NonEventSourcedActor_IsNeverPassivated()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(20), TimeSpan.FromHours(1));
        try
        {
            system.Register<FixPPlainActor>(cmd => new FixPPlainActor((FixPNoOp)cmd));

            var actor = await system.CreateAsync<FixPPlainActor>(new FixPNoOp());
            var id = actor.Id;
            var baseline = system.PassivatedCount;

            var passivated = await SweepUntilAsync(
                system,
                () => system.PassivatedCount > baseline,
                300
            );
            await Assert.That(passivated).IsFalse();

            // Still registered and still taking commands (a wrongly passivated non-ES actor
            // would throw KeyNotFoundException here).
            await Assert.That(() => system.Send(id, new FixPNoOp())).ThrowsNothing();
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P1 + P3: an actor in the middle of a command turn is not idle. Once the turn ends
    /// and the actor has been idle for a full timeout, it becomes a candidate.
    /// </summary>
    [Test]
    public async Task ActorProcessingLongCommand_IsNotPassivated_UntilItGoesIdle()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(50), TimeSpan.FromHours(1));
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            var entered = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var release = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var parked = system.AskAsync<object?>(id, new FixPPark(entered, release)).AsTask();

            await entered.Task;
            await Assert.That(actor.IsProcessing).IsTrue();
            await Assert.That(actor.PendingCount).IsEqualTo(0);

            var baseline = system.PassivatedCount;
            var passivatedWhileBusy = await SweepUntilAsync(
                system,
                () => system.PassivatedCount > baseline,
                300
            );
            await Assert.That(passivatedWhileBusy).IsFalse();
            await Assert.That(parked.IsCompleted).IsFalse();

            release.TrySetResult(true);
            await parked;

            var passivatedWhenIdle = await SweepUntilAsync(
                system,
                () => system.PassivatedCount > baseline
            );
            await Assert.That(passivatedWhenIdle).IsTrue();
            await Assert.That(await WaitUntilAsync(() => IsGone(system, id))).IsTrue();
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P3: an actor with a backlog is not idle either. The queued command must survive
    /// (the passivation decision is taken before it is enqueued behind a sentinel).
    /// </summary>
    [Test]
    public async Task ActorWithBacklog_IsNotPassivated_AndKeepsTheBacklog()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(20), TimeSpan.FromHours(1));
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            var entered = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var release = new TaskCompletionSource<bool>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var parked = system.AskAsync<object?>(id, new FixPPark(entered, release)).AsTask();
            await entered.Task;

            // Backlog: one command waiting behind the parked turn.
            system.Send(id, new FixPIncrement(5));
            var queued = await WaitUntilAsync(() => actor.PendingCount > 0, 1_000);
            await Assert.That(queued).IsTrue();

            var baseline = system.PassivatedCount;
            var passivatedWithBacklog = await SweepUntilAsync(
                system,
                () => system.PassivatedCount > baseline,
                300
            );
            await Assert.That(passivatedWithBacklog).IsFalse();

            release.TrySetResult(true);
            await parked;

            // The backlogged increment ran: nothing was dropped by a passivation.
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(5);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P2: passivation is opt-in. With the default (IdleTimeout = null) nothing is ever
    /// reclaimed, even for a perfectly rebuildable ES actor.
    /// </summary>
    [Test]
    public async Task DefaultOptions_NeverPassivate()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, null, null);
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            var passivated = await SweepUntilAsync(system, () => system.PassivatedCount > 0, 300);
            await Assert.That(passivated).IsFalse();
            await Assert.That(system.PassivatedCount).IsEqualTo(0);

            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(0);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P3: the sweep is safe to run any number of times, including against an id that was
    /// already passivated (repeated requests must not throw or resurrect anything).
    /// </summary>
    [Test]
    public async Task RepeatedSweeps_AreIdempotent()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(1), TimeSpan.FromHours(1));
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            var baseline = system.PassivatedCount;

            // Two sweeps in a row may both issue a passivation request for the same actor.
            await system.SweepIdleActorsAsync();
            await system.SweepIdleActorsAsync();
            await system.SweepIdleActorsAsync();

            // Poll + cap: the idle budget is 1ms, so a sweep issued "too early" is simply retried
            // by the next one (no background timer here — the interval is an hour).
            var passivated = await SweepUntilAsync(system, () => system.PassivatedCount > baseline);
            await Assert.That(passivated).IsTrue();
            await Assert.That(await WaitUntilAsync(() => IsGone(system, id))).IsTrue();

            // Registry miss x5: must be a no-op, not a throw.
            for (var i = 0; i < 5; i++)
                await system.SweepIdleActorsAsync();

            await Assert.That(IsGone(system, id)).IsTrue();

            // ... and an already-passivated id still rebuilds exactly once.
            var first = await system.GetAsync<FixPCounter>(id);
            var second = await system.GetAsync<FixPCounter>(id);
            await Assert.That(first).IsNotNull();
            await Assert.That(ReferenceEquals(first, second)).IsTrue();
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P3/P4: passivation only drops the in-memory copy. The stream is untouched, and a
    /// rebuild replays every event — the aggregate is exactly where it was.
    /// </summary>
    [Test]
    public async Task Passivation_KeepsTheEventStreamIntact()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(50), TimeSpan.FromHours(1));
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;

            system.Send(id, new FixPIncrement(1));
            system.Send(id, new FixPIncrement(2));
            system.Send(id, new FixPIncrement(3));
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(6);

            var before = await store.LoadAsync(id);
            await Assert.That(before.Count).IsEqualTo(4); // created + three increments
            var baseline = system.PassivatedCount;

            var passivated = await SweepUntilAsync(system, () => system.PassivatedCount > baseline);
            await Assert.That(passivated).IsTrue();
            await Assert.That(await WaitUntilAsync(() => IsGone(system, id))).IsTrue();

            var after = await store.LoadAsync(id);
            await Assert.That(after.Count).IsEqualTo(before.Count);

            var rebuilt = await system.GetAsync<FixPCounter>(id);
            await Assert.That(rebuilt).IsNotNull();
            await Assert.That(rebuilt!.Version).IsEqualTo(4UL);
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(6);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// I-1: a saga WAITING for an external reply must never be reclaimed. It is registered with
    /// a rebuild factory (so on paper it IS a candidate) and it is idle by definition while it
    /// waits — but its state is not a cache of a finished aggregate. Passivating it means the
    /// reply command arrives at an id nobody owns (KeyNotFoundException) and the saga is stuck
    /// mid-flight for good. A plain ES actor in the SAME system is still reclaimed, so the
    /// exemption belongs to the saga, it does not disable the feature.
    /// </summary>
    [Test]
    public async Task SagaWaitingForReply_IsNeverPassivated_WhilePlainEsActorStillIs()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromMilliseconds(200), TimeSpan.FromHours(1));
        try
        {
            system.Register<FixPWaitingSaga>(
                _ => new FixPWaitingSaga(),
                () => new FixPWaitingSaga()
            );
            RegisterCounter(system);

            // A saga takes no creation-time command: it is created empty and then advanced
            // through its mailbox, exactly like production code does.
            var saga = await system.CreateAsync<FixPWaitingSaga>(new FixPSagaStart("waiting"));
            var sagaId = saga.Id;
            await Assert
                .That(await system.AskAsync<object?>(sagaId, new FixPSagaStart("waiting")))
                .IsNull();
            await Assert
                .That(await system.AskAsync<string>(sagaId, new FixPSagaReply("hello")))
                .IsEqualTo("hello");

            var counter = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var counterId = counter.Id;

            // Control: the plain ES actor in the same system IS reclaimed. The sweep is driven
            // by hand — the configured interval is an hour, so no timer decides anything here.
            var baseline = system.PassivatedCount;
            await Assert
                .That(await SweepUntilAsync(system, () => system.PassivatedCount > baseline))
                .IsTrue();
            await Assert.That(await WaitUntilAsync(() => IsGone(system, counterId))).IsTrue();

            // ... while the idle saga is untouched: waiting is its normal state, and the reply
            // it is waiting for must still reach the SAME id.
            await Assert.That(IsGone(system, sagaId)).IsFalse();
            await Assert
                .That(await system.AskAsync<string>(sagaId, new FixPSagaName()))
                .IsEqualTo("waiting");
            await Assert
                .That(await system.AskAsync<string>(sagaId, new FixPSagaReply("still here")))
                .IsEqualTo("still here");
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// m-2: with passivation OFF (the default) nothing ever reads the idle clock, so the hot
    /// paths — every <c>Post</c>, and the end of every dispatched command — must not pay for a
    /// <c>DateTime.UtcNow</c> that has no reader.
    /// </summary>
    [Test]
    public async Task PassivationDisabled_LeavesTheIdleClockUntouched()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, null, null);
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var before = actor.LastActivityUtc;

            await Task.Delay(50);
            system.Send(actor.Id, new FixPIncrement(1));
            await Assert
                .That(await system.AskAsync<object?>(actor.Id, new FixPIncrement(1)))
                .IsNull();
            await Assert
                .That(await system.AskAsync<int>(actor.Id, new FixPGetValue()))
                .IsEqualTo(2);

            await Assert.That(actor.LastActivityUtc).IsEqualTo(before);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// m-2, other half: when passivation IS enabled the clock must still move, or the sweeper
    /// would reclaim a working actor.
    /// </summary>
    [Test]
    public async Task PassivationEnabled_TracksIdleActivity()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var before = actor.LastActivityUtc;

            await Task.Delay(50);
            await Assert
                .That(await system.AskAsync<object?>(actor.Id, new FixPIncrement(1)))
                .IsNull();

            await Assert.That(actor.LastActivityUtc).IsGreaterThan(before);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// m-9: the passivation request is a mailbox write, and a write to a CLOSED mailbox fails.
    /// The caller (the sweeper) must be able to tell, instead of counting — and logging — a
    /// reclaim that never happened.
    /// </summary>
    [Test]
    public async Task RequestPassivation_ReportsWhetherTheRequestWasQueued()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store, null, null);
        try
        {
            RegisterCounter(system);

            var stopping = await system.CreateAsync<FixPCounter>(new FixPCreate());
            await system.StopAsync(stopping.Id);

            // Closed mailbox (the loop already exited): the request was NOT queued.
            await Assert.That(stopping.RequestPassivation()).IsFalse();

            // A live actor accepts it, and reclaims itself at that message boundary.
            var live = await system.CreateAsync<FixPCounter>(new FixPCreate());
            await Assert.That(live.RequestPassivation()).IsTrue();
            await Assert.That(await WaitUntilAsync(() => IsGone(system, live.Id))).IsTrue();
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// P2: a non-positive IdleTimeout is a configuration bug, not "disabled" — refuse it
    /// at construction instead of sweeping with a meaningless budget.
    /// </summary>
    [Test]
    public async Task NonPositiveIdleTimeout_IsRejected()
    {
        var store = new InMemoryEventStore();

        await Assert
            .That(() => NewSystem(store, TimeSpan.Zero, null))
            .Throws<ArgumentOutOfRangeException>();
        await Assert
            .That(() => NewSystem(store, TimeSpan.FromMilliseconds(-1), null))
            .Throws<ArgumentOutOfRangeException>();
    }
}
