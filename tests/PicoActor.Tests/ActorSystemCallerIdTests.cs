namespace PicoActor.Tests;

public sealed class ActorSystemCallerIdTests
{
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
    public async Task CreateAsync_DefaultOverload_StillGeneratesIds()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        RegisterSimple(system);

        var a = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        var b = await system.CreateAsync<SimpleActor>(new NoOpCmd());

        await Assert.That(a.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(b.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(a.Id).IsNotEqualTo(b.Id);
    }

    [Test]
    public async Task ActorSystem_WithNullOptions_ThrowsArgumentNullException()
    {
        await Assert.That(() => new ActorSystem(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task AskAsync_ContinuationDoesNotRunOnActorLoopThread()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<ThreadProbeActor>(
            cmd =>
                cmd switch
                {
                    NoOpCmd => new ThreadProbeActor((NoOpCmd)cmd),
                    _ => throw new InvalidOperationException(),
                },
            () => new ThreadProbeActor()
        );

        var probe = await system.CreateAsync<ThreadProbeActor>(new NoOpCmd());

        using var gate = new ManualResetEventSlim();
        try
        {
            // Envelope order: ask first, then a blocking message. The loop
            // completes the ask's TCS and immediately blocks itself on the gate
            // (never returning to the pool), so the queued continuation must run
            // on another thread. An inline continuation would run on the loop
            // thread while it is still inside the TCS completion.
            var askTask = system.AskAsync<int>(probe.Id, new GetLoopThreadId());
            system.Send(probe.Id, new BlockLoopCmd(gate));

            var loopThreadId = await askTask;
            await Assert.That(Environment.CurrentManagedThreadId).IsNotEqualTo(loopThreadId);
        }
        finally
        {
            gate.Set(); // release the loop so the actor can stop cleanly
        }

        await system.StopAsync(probe.Id);
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
