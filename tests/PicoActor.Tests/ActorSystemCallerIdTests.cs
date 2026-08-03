using PicoActor.Abs;

namespace PicoActor.Tests;

public sealed class ActorSystemCallerIdTests
{
    private static readonly Guid FixedId = Guid.Parse("00000000-0000-0000-0000-0000000000a1");

    private static void RegisterSimple(ActorSystem system)
    {
        system.Register<SimpleActor>(cmd =>
            cmd switch
            {
                NoOpCmd => new SimpleActor((NoOpCmd)cmd),
                _ => throw new InvalidOperationException(),
            }
        );
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_AssignsId()
    {
        var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
        RegisterSimple(system);

        var actor = await system.CreateAsync<SimpleActor>(new NoOpCmd(), FixedId);

        await Assert.That(actor.Id).IsEqualTo(FixedId);
        var viaGet = await system.GetAsync<SimpleActor>(FixedId);
        await Assert.That(viaGet).IsNotNull();
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_DuplicateId_ThrowsAndSystemUsable()
    {
        var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
        RegisterSimple(system);

        await system.CreateAsync<SimpleActor>(new NoOpCmd(), FixedId);

        await Assert
            .That(async () => await system.CreateAsync<SimpleActor>(new NoOpCmd(), FixedId))
            .Throws<InvalidOperationException>();

        // System remains usable
        var other = await system.CreateAsync<SimpleActor>(
            new NoOpCmd(),
            Guid.Parse("00000000-0000-0000-0000-0000000000a2")
        );
        await Assert.That(other.Id).IsNotEqualTo(FixedId);
    }

    private static void RegisterCounter(ActorSystem system)
    {
        system.Register<Counter>(
            cmd =>
                cmd switch
                {
                    CreateCounter c => new Counter(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new Counter()
        );
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_EventSourced_PersistsUnderThatId()
    {
        var store = new InMemoryEventStore();
        var systemA = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(systemA);
        await systemA.CreateAsync<Counter>(new CreateCounter(7), FixedId);

        // A fresh system over the same store must rebuild the same actor by id
        var systemB = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(systemB);
        var rebuilt = await systemB.GetAsync<Counter>(FixedId);
        await Assert.That(rebuilt).IsNotNull();
        var value = await systemB.AskAsync<int>(FixedId, new GetValue());
        await Assert.That(value).IsEqualTo(7);
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_StoreAlreadyHasEventsForId_Throws()
    {
        var store = new InMemoryEventStore();
        var systemA = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(systemA);
        await systemA.CreateAsync<Counter>(new CreateCounter(1), FixedId);

        // "Second process" boot over the same store: the aggregate file already
        // exists — CreateAsync(id) must fail loudly, not silently overwrite.
        var systemB = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(systemB);
        await Assert
            .That(async () => await systemB.CreateAsync<Counter>(new CreateCounter(2), FixedId))
            .Throws<ConcurrencyException>();

        // System remains usable
        var other = await systemB.CreateAsync<Counter>(
            new CreateCounter(3),
            Guid.Parse("00000000-0000-0000-0000-0000000000a3")
        );
        await Assert.That(other.Id).IsNotEqualTo(FixedId);
    }

    [Test]
    public async Task CreateAsync_DefaultOverload_StillGeneratesIds()
    {
        var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
        RegisterSimple(system);

        var a = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        var b = await system.CreateAsync<SimpleActor>(new NoOpCmd());

        await Assert.That(a.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(b.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(a.Id).IsNotEqualTo(b.Id);
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_DuplicateId_EventSourced_ThrowsWithoutCorruptingStore()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(system);

        await system.CreateAsync<Counter>(new CreateCounter(1), FixedId);

        // ES duplicate: the discarded actor staged CounterCreated in its constructor.
        // It must NOT flush to the store (which would race the winner's flush), and
        // the documented exception type must surface — not ConcurrencyException
        // from the loser's init flush.
        await Assert
            .That(async () => await system.CreateAsync<Counter>(new CreateCounter(2), FixedId))
            .Throws<InvalidOperationException>();

        // The stream holds exactly ONE event batch — no silent duplicate append
        var events = await store.LoadAsync(FixedId);
        await Assert.That(events.Count).IsEqualTo(1);

        // The winner remains functional
        var value = await system.AskAsync<int>(FixedId, new GetValue());
        await Assert.That(value).IsEqualTo(1);
    }

    [Test]
    public async Task ActorSystem_WithNullOptions_ThrowsArgumentNullException()
    {
        await Assert
            .That(() => new ActorSystem(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task AskAsync_ContinuationDoesNotRunOnActorLoopThread()
    {
        var system = new ActorSystem(new ActorSystemOptions { EventStore = new InMemoryEventStore() });
        system.Register<ThreadProbeActor>(
            cmd =>
                cmd switch
                {
                    NoOpCmd => new ThreadProbeActor((NoOpCmd)cmd),
                    _ => throw new InvalidOperationException(),
                },
            () => new ThreadProbeActor()
        );

        await system.CreateAsync<ThreadProbeActor>(new NoOpCmd(), FixedId);

        using var gate = new ManualResetEventSlim();
        try
        {
            // Envelope order: ask first, then a blocking message. The loop
            // completes the ask's TCS and immediately blocks itself on the gate
            // (never returning to the pool), so the queued continuation must run
            // on another thread. An inline continuation would run on the loop
            // thread while it is still inside the TCS completion.
            var askTask = system.AskAsync<int>(FixedId, new GetLoopThreadId());
            system.Send(FixedId, new BlockLoopCmd(gate));

            var loopThreadId = await askTask;
            await Assert.That(Environment.CurrentManagedThreadId).IsNotEqualTo(loopThreadId);
        }
        finally
        {
            gate.Set(); // release the loop so the actor can stop cleanly
        }

        await system.StopAsync(FixedId);
    }

    [Test]
    public async Task AppendAsync_ConcurrentWriters_SameExpectedVersion_ExactlyOneSucceeds()
    {
        var store = new InMemoryEventStore();
        var id = Guid.Parse("00000000-0000-0000-0000-0000000000a5");
        // Large batches widen the check-then-act window (AddRange copy time) so
        // the TOCTOU race reproduces reliably on the unfixed store.
        var events = Enumerable
            .Range(0, 10_000)
            .Select<int, IDomainEvent>(i => new CounterCreated(i))
            .ToList();

        // 20 concurrent writers, all expecting version 0 — the check-then-act must
        // be atomic per actor: exactly one wins, the rest get ConcurrencyException.
        var results = await Task.WhenAll(
            Enumerable.Range(0, 20).Select(_ => TryAppendAsync(store, id, events))
        );

        await Assert.That(results.Count(r => r.Success)).IsEqualTo(1);
        await Assert.That(results.Count(r => r.VersionConflict)).IsEqualTo(19);

        // Exactly one winner's full batch — no interleaved/duplicated events
        var stream = await store.LoadAsync(id);
        await Assert.That(stream.Count).IsEqualTo(events.Count);
    }

    private static async Task<(bool Success, bool VersionConflict)> TryAppendAsync(
        InMemoryEventStore store,
        Guid id,
        List<IDomainEvent> events
    )
    {
        try
        {
            await store.AppendAsync(id, 0, events);
            return (true, false);
        }
        catch (ConcurrencyException)
        {
            return (false, true);
        }
    }
}
