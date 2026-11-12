namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// I-2 — a passivation request must stop the instance that ASKED for it, never
// "whatever currently owns that id".
//
// The request travels through the mailbox (a sentinel), which is what makes it
// safe against a command racing the reclaim. That does not make it safe against
// the REGISTRY: the sentinel is consumed by a loop, while the id it names may
// already belong to a DIFFERENT instance (a rebuild after a concurrent stop, for
// example). Removing by id alone would unregister that live actor — its loop
// keeps running and nothing can reach it again — while the actor that actually
// wanted to go away stays registered.
//
// The tests pin the host-side contract the runtime uses, which is exactly the
// call the sentinel path makes: TryRequestStop(id, theInstanceThatAsked).
// ═══════════════════════════════════════════════════════════

public sealed class FixSPStaleStopTests
{
    private static ActorSystem NewSystem(InMemoryEventStore store) =>
        new(new ActorSystemOptions { EventStore = store, StopTimeout = TimeSpan.FromSeconds(5) });

    private static void RegisterCounter(ActorSystem system) =>
        system.Register<FixPCounter>(
            cmd =>
                cmd is FixPCreate create
                    ? new FixPCounter(create)
                    : throw new InvalidOperationException($"unexpected {cmd.GetType().Name}"),
            () => new FixPCounter()
        );

    /// <summary>
    /// The stale request: instance A asked to be reclaimed, and by the time the request is acted
    /// on, the id belongs to a rebuilt instance B. B must survive — registered, initialized and
    /// still holding the replayed state.
    /// </summary>
    [Test]
    public async Task StaleStopRequest_LeavesTheLiveInstanceRegistered()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store);
        try
        {
            RegisterCounter(system);

            var first = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = first.Id;
            system.Send(id, new FixPIncrement(5));
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(5);

            // A goes away (explicit stop) and the SAME id comes back as a rebuilt instance —
            // the situation a late passivation sentinel from A lands in.
            await system.StopAsync(id);
            var rebuilt = await system.GetAsync<FixPCounter>(id);
            await Assert.That(rebuilt).IsNotNull();
            await Assert.That(rebuilt!.Id).IsEqualTo(id);

            var host = (IActorPassivationHost)system;

            // The request names the OLD instance: it must not unregister the new one.
            await Assert.That(host.TryRequestStop(id, first)).IsFalse();

            // B still owns the id, still answers, and still carries the replayed state.
            await Assert
                .That(ReferenceEquals(await system.GetAsync<FixPCounter>(id), rebuilt))
                .IsTrue();
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(5);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }

    /// <summary>
    /// The honest case: the instance that owns the id asks for its own stop. The registry entry
    /// goes away (Send throws, exactly as a passivated id behaves) and the loop is signalled —
    /// while the event stream stays untouched.
    /// </summary>
    [Test]
    public async Task CurrentInstanceStopRequest_RemovesAndSignalsIt()
    {
        var store = new InMemoryEventStore();
        var system = NewSystem(store);
        try
        {
            RegisterCounter(system);

            var actor = await system.CreateAsync<FixPCounter>(new FixPCreate());
            var id = actor.Id;
            await Assert.That(await system.AskAsync<int>(id, new FixPGetValue())).IsEqualTo(0);

            var host = (IActorPassivationHost)system;
            await Assert.That(host.TryRequestStop(id, actor)).IsTrue();

            await Assert
                .That(() => system.Send(id, new FixPIncrement(1)))
                .Throws<KeyNotFoundException>();

            // A passivation, not a delete: the stream is the truth and it is still there.
            await Assert.That((await store.LoadAsync(id)).Count).IsEqualTo(1);
        }
        finally
        {
            await system.StopAllAsync();
        }
    }
}
