namespace PicoActor.Tests;

// Cross-feature check: passivation must not break snapshot-accelerated recovery.
// Each feature was developed and tested alone; this pins the interaction that neither
// branch could cover — an idle actor is reclaimed, and its rebuild must still use the
// snapshot (only the tail reaches Mutate), not the whole stream.

internal sealed record SpaCreate(int Start) : ICommand;

internal sealed record SpaAdd(int Delta) : ICommand;

internal sealed record SpaGet : ICommand;

internal sealed record SpaValueSet(int Value) : IDomainEvent;

internal sealed record SpaValueAdded(int Delta) : IDomainEvent;

internal sealed class SpaCounter : EventSourcedActor
{
    private int _value;

    public SpaCounter(SpaCreate cmd)
        : base(cmd) { }

    public SpaCounter() { }

    /// <summary>Mutate calls observed by THIS instance (a rebuild gets a fresh instance).</summary>
    public int MutateCalls { get; private set; }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case SpaCreate c:
                RaiseEvent(new SpaValueSet(c.Start));
                break;
            case SpaAdd a:
                RaiseEvent(new SpaValueAdded(a.Delta));
                break;
            case SpaGet:
                return new ValueTask<object?>(_value);
        }

        return new ValueTask<object?>((object?)null);
    }

    protected override void Mutate(IDomainEvent @event)
    {
        MutateCalls++;
        switch (@event)
        {
            case SpaValueSet e:
                _value = e.Value;
                break;
            case SpaValueAdded e:
                _value += e.Delta;
                break;
        }
    }

    // Snapshot payload = the counter (4 bytes); the version is carried by the store.
    protected override byte[]? TakeSnapshot() => BitConverter.GetBytes(_value);

    protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload) =>
        _value = BitConverter.ToInt32(payload);
}

public sealed class FixSPIntegrationTests
{
    [Test]
    [Timeout(20000)]
    public async Task PassivatedActor_RebuildsFromSnapshot_AndReplaysOnlyTheTail(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(
            new ActorSystemOptions
            {
                EventStore = store,
                SnapshotEveryNEvents = 5,
                IdleTimeout = TimeSpan.FromMilliseconds(100),
                IdleSweepInterval = TimeSpan.FromMilliseconds(25),
            }
        );
        system.Register<SpaCounter>(cmd => new SpaCounter((SpaCreate)cmd), () => new SpaCounter());

        var actor = await system.CreateAsync<SpaCounter>(new SpaCreate(0));
        var id = actor.Id;

        // 1 creation event + 11 increments = 12 persisted events; a snapshot lands at 10.
        for (var i = 0; i < 11; i++)
            await system.AskAsync<object?>(id, new SpaAdd(1));

        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(12);
        var snapshot = await store.LoadSnapshotAsync(id);
        await Assert.That(snapshot).IsNotNull();
        await Assert.That(snapshot!.Version).IsEqualTo((ulong)10);

        // Idle passivation. The counter only proves the request was QUEUED (that is what the
        // sweeper counts); the registry entry disappears later, when the loop reaches the
        // sentinel. So the wait has to be on the OBSERVABLE effect, with a cap — asserting the
        // KeyNotFoundException right after the counter moves is a timing assumption, not a
        // property. The probe is read-only (SpaGet raises no event and changes no state);
        // InvalidOperationException is the stop-race window (mailbox closed, entry not yet
        // removed) and is retried.
        var deadline = Environment.TickCount64 + 5_000;
        while (system.PassivatedCount == 0 && Environment.TickCount64 < deadline)
            await Task.Delay(20, cancellationToken);
        await Assert.That(system.PassivatedCount).IsGreaterThan(0);

        var gone = false;
        while (!gone && Environment.TickCount64 < deadline)
        {
            try
            {
                system.Send(id, new SpaGet());
            }
            catch (KeyNotFoundException)
            {
                gone = true;
                break;
            }
            catch (InvalidOperationException)
            {
                // Stopping: retry until the registry entry is gone.
            }

            await Task.Delay(10, cancellationToken);
        }

        await Assert.That(gone).IsTrue();
        await Assert.That(() => system.Send(id, new SpaAdd(1))).Throws<KeyNotFoundException>();

        // Rebuild: snapshot + tail only. 12 events, snapshot at 10 => 2 Mutate calls.
        var rebuilt = await system.GetAsync<SpaCounter>(id);
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Version).IsEqualTo((ulong)12);
        await Assert.That(rebuilt.MutateCalls).IsEqualTo(2);
        await Assert.That(await system.AskAsync<int>(id, new SpaGet())).IsEqualTo(11);

        // Reactivation counter and continued operation after the rebuild.
        await Assert.That(system.ReactivatedCount).IsGreaterThan(0);
        await system.AskAsync<object?>(id, new SpaAdd(4));
        await Assert.That(await system.AskAsync<int>(id, new SpaGet())).IsEqualTo(15);

        // History is never truncated by snapshotting or passivation.
        await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(13);
    }
}
