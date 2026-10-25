namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// J5 (review M-5) — the GetAsync REBUILD diagnostics are best effort.
//
// GetAsync rebuilds an event-sourced actor and then reports the outcome. By the time that
// report runs the instance is rebuilt, initialized and REGISTERED: letting a broken log sink
// throw out of GetAsync would hand the caller an exception for a lookup that succeeded, with
// a live actor nobody holds a handle to (reachable only by id, never stopped). Both reports
// on that path — "rebuilt from events" and the duplicate-rebuild warning — go through
// TryLog, and this test pins the observable consequence of the first one.
// ═══════════════════════════════════════════════════════════

public sealed class FixJGetAsyncLoggingTests
{
    /// <summary>
    /// A rebuild that SUCCEEDS with a log sink that throws on every report must still return
    /// the instance, keep it registered, and leave it answering commands.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task GetAsync_RebuildSuccess_ThrowingLogSink_StillReturnsAUsableInstance(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var log = new FixBPoisonLogSink();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store, Logger = log });
        system.Register<Counter>(cmd => new Counter((CreateCounter)cmd), () => new Counter());

        // A real stream to rebuild from: create + one mutation (2 events), then stop so the
        // next GetAsync has to take the rebuild path.
        var original = await system.CreateAsync<Counter>(new CreateCounter(10));
        system.Send(original.Id, new Increment(5));
        await Assert.That(await system.AskAsync<int>(original.Id, new GetValue())).IsEqualTo(15);
        await system.StopAsync(original.Id);

        var callsBefore = log.Calls;
        log.Arm(); // every report throws from here on — including the rebuild success line

        Counter? rebuilt = null;
        Exception? thrown = null;
        try
        {
            rebuilt = await system.GetAsync<Counter>(original.Id);
        }
        catch (Exception ex)
        {
            // The bug: an IOException from the sink, AFTER the actor was rebuilt.
            thrown = ex;
        }

        await Assert.That(thrown).IsNull();
        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Id).IsEqualTo(original.Id);

        // The diagnostic really was attempted and swallowed (not deleted outright).
        await Assert.That(log.Calls).IsGreaterThan(callsBefore);

        // The rebuilt state came from the persisted events, and the instance is usable.
        await Assert.That(await system.AskAsync<int>(rebuilt.Id, new GetValue())).IsEqualTo(15);

        // ... and it is the REGISTERED owner: a second lookup returns the same instance
        // instead of rebuilding a second copy.
        var again = await system.GetAsync<Counter>(original.Id);
        await Assert.That(ReferenceEquals(again, rebuilt)).IsTrue();

        await system.StopAsync(rebuilt.Id);
    }
}
