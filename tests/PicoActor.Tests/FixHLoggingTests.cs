namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// H2 — the CreateAsync SUCCESS diagnostic is best effort. The actor is already
// registered and initialized when that line runs, so a throwing log sink used to
// escape CreateAsync and hand the caller an exception for a creation that
// SUCCEEDED — while the registry kept the actor: the caller has no handle, but the
// actor runs (unreachable except through GetAsync, never stopped by id).
// Same shape as StopAsync/StopAllAsync: report through TryLog.
// ═══════════════════════════════════════════════════════════

public sealed class FixHLoggingTests
{
    /// <summary>
    /// A sink that throws on EVERY report must not change CreateAsync's outcome: the
    /// actor is returned, it is registered, and it answers messages. The sink is still
    /// consulted (the diagnostic is attempted, not deleted) — its failure is swallowed.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task CreateAsync_ThrowingLogSink_StillReturnsAUsableActor(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBPoisonLogSink();
        log.Arm(); // every report throws from here on — including the creation success line

        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore(), Logger = log }
        );
        system.Register<SimpleActor>(cmd => new SimpleActor((NoOpCmd)cmd));

        SimpleActor? actor = null;
        Exception? thrown = null;
        try
        {
            actor = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        }
        catch (Exception ex)
        {
            // The bug: an IOException from the sink — AFTER the actor was created.
            thrown = ex;
        }

        await Assert.That(thrown).IsNull();
        await Assert.That(actor).IsNotNull();
        await Assert.That(actor!.Id).IsNotEqualTo(Guid.Empty);

        // The diagnostic really was attempted and swallowed (not silently dropped).
        await Assert.That(log.Calls).IsGreaterThan(0);

        // The returned handle is the REGISTERED instance — the registry kept it all along,
        // so a failed CreateAsync left it orphaned.
        var registered = await system.GetAsync<SimpleActor>(actor.Id);
        await Assert.That(registered).IsNotNull();
        await Assert.That(ReferenceEquals(registered, actor)).IsTrue();

        // ... and it is alive: the mailbox accepts and answers commands.
        await Assert.That(await system.AskAsync<object?>(actor.Id, new NoOpCmd())).IsNull();

        // Clean shutdown of the actor it handed out (also exercises the stop diagnostics
        // against the same throwing sink).
        await system.StopAsync(actor.Id);
        await Assert
            .That(() => system.Send(actor.Id, new NoOpCmd()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// The init-FAILURE diagnostic has the same shape and a worse consequence: it runs
    /// inside the catch block that rethrows the original initialization exception, so a
    /// throwing sink replaced that exception (the caller could no longer tell WHY creation
    /// failed) and skipped the whole failure cleanup — the failed instance stayed in the
    /// registry, unreachable but never stopped.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task CreateAsync_InitFailure_ThrowingLogSink_StillRaisesTheInitFailure(
        CancellationToken cancellationToken = default
    )
    {
        var log = new FixBPoisonLogSink();
        log.Arm();

        var store = new FixBBlockingAppendStore(Array.Empty<IDomainEvent>());
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store, Logger = log });
        system.Register<FixBOwnershipActor>(cmd => new FixBOwnershipActor((FixBCreate)cmd));

        var createTask = system.CreateAsync<FixBOwnershipActor>(new FixBCreate(1)).AsTask();
        store.Release(); // the first (creation) append fails

        Exception? thrown = null;
        try
        {
            await createTask;
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        // The ORIGINAL failure, not the sink's IOException.
        await Assert.That(thrown).IsNotNull();
        await Assert.That(thrown!.Message).Contains("append failed after release");
        await Assert.That(log.Calls).IsGreaterThan(0);

        // The failure cleanup ran: the dead instance left the registry.
        await Assert.That(await system.GetAsync<FixBOwnershipActor>(store.BlockedActorId)).IsNull();
    }
}
