namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A1 — a command handler that throws must not leak the events it already raised
// into the next command's flush. The failed batch was never persisted, so leaving
// it in the uncommitted list corrupts the stream (the next successful append
// silently persists the failed command's events) and the in-memory Version.
// ═══════════════════════════════════════════════════════════

internal sealed record FixACreated(int Value) : ICommand;

internal sealed record FixAAdd(int Delta) : ICommand;

internal sealed record FixARaiseThenThrow(int Delta) : ICommand;

internal sealed record FixARaiseTwoThenThrow : ICommand;

internal sealed record FixAThrowOnly : ICommand;

internal sealed record FixAReadValue : ICommand;

internal sealed record FixAValueSet(int Value) : IDomainEvent;

internal sealed record FixAValueAdded(int Delta) : IDomainEvent;

internal sealed class FixACounter : EventSourcedActor
{
    private int _value;

    public FixACounter(FixACreated cmd)
        : base(cmd) { }

    public FixACounter() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixACreated c:
                RaiseEvent(new FixAValueSet(c.Value));
                return default;
            case FixAAdd a:
                RaiseEvent(new FixAValueAdded(a.Delta));
                return default;
            case FixARaiseThenThrow t:
                RaiseEvent(new FixAValueAdded(t.Delta));
                throw new InvalidOperationException("handler boom");
            case FixARaiseTwoThenThrow:
                RaiseEvent(new FixAValueAdded(101));
                RaiseEvent(new FixAValueAdded(102));
                throw new InvalidOperationException("handler boom");
            case FixAThrowOnly:
                throw new InvalidOperationException("handler boom");
            case FixAReadValue:
                return new ValueTask<object?>(_value);
            default:
                return default;
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case FixAValueSet e:
                _value = e.Value;
                break;
            case FixAValueAdded e:
                _value += e.Delta;
                break;
        }
    }
}

public sealed class FixADispatchRollbackTests
{
    private static (ActorSystem System, InMemoryEventStore Store) NewSystem()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixACounter>(
            cmd => new FixACounter((FixACreated)cmd),
            () => new FixACounter()
        );
        return (system, store);
    }

    /// <summary>
    /// RaiseEvent then throw: the raised event belongs to a batch that was never
    /// persisted. It must be dropped from the uncommitted list AND the Version must be
    /// rolled back, so the next successful command appends exactly its own events at
    /// the right expectedVersion.
    /// </summary>
    [Test]
    public async Task ThrowingHandler_DropsItsEvents_AndNextCommandStaysClean()
    {
        var (system, store) = NewSystem();

        var actor = await system.CreateAsync<FixACounter>(new FixACreated(10));
        var id = actor.Id;

        var created = await store.LoadAsync(id);
        await Assert.That(created.Count).IsEqualTo(1);

        // Handler raises an event, then throws — the caller must see the original exception.
        await Assert
            .That(async () => await system.AskAsync<object?>(id, new FixARaiseThenThrow(-5)))
            .Throws<InvalidOperationException>();

        // A later healthy command must persist ONLY its own event.
        await system.AskAsync<object?>(id, new FixAAdd(3));

        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(events[0]).IsTypeOf<FixAValueSet>();
        await Assert.That(events[1]).IsTypeOf<FixAValueAdded>();
        await Assert.That(((FixAValueAdded)events[1]).Delta).IsEqualTo(3);

        // Version tracks the persisted stream, not the abandoned batch.
        await Assert.That(actor.Version).IsEqualTo((ulong)events.Count);
        await Assert.That(await system.AskAsync<int>(id, new FixAReadValue())).IsEqualTo(13);

        // The event stream is authoritative: a rebuild reproduces 13, not -2.
        await system.StopAsync(id);
        var rebuilt = await system.GetAsync<FixACounter>(id);
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(await system.AskAsync<int>(id, new FixAReadValue())).IsEqualTo(13);
    }

    /// <summary>A multi-event batch that throws midway is discarded as a whole.</summary>
    [Test]
    public async Task ThrowingHandler_DropsWholePartialBatch()
    {
        var (system, store) = NewSystem();

        var actor = await system.CreateAsync<FixACounter>(new FixACreated(0));
        var id = actor.Id;

        await Assert
            .That(async () => await system.AskAsync<object?>(id, new FixARaiseTwoThenThrow()))
            .Throws<InvalidOperationException>();

        await system.AskAsync<object?>(id, new FixAAdd(7));

        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(((FixAValueAdded)events[1]).Delta).IsEqualTo(7);

        await Assert.That(actor.Version).IsEqualTo((ulong)events.Count);
        await Assert.That(await system.AskAsync<int>(id, new FixAReadValue())).IsEqualTo(7);
    }

    /// <summary>Throwing before any RaiseEvent changes nothing at all.</summary>
    [Test]
    public async Task ThrowingHandler_WithoutRaisedEvents_LeavesActorUntouched()
    {
        var (system, store) = NewSystem();

        var actor = await system.CreateAsync<FixACounter>(new FixACreated(5));
        var id = actor.Id;
        var versionBefore = actor.Version;

        await Assert
            .That(async () => await system.AskAsync<object?>(id, new FixAThrowOnly()))
            .Throws<InvalidOperationException>();

        await Assert.That(actor.Version).IsEqualTo(versionBefore);

        await system.AskAsync<object?>(id, new FixAAdd(1));

        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(2);
        await Assert.That(actor.Version).IsEqualTo((ulong)events.Count);
        await Assert.That(await system.AskAsync<int>(id, new FixAReadValue())).IsEqualTo(6);
    }
}
