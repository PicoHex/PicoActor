using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

public sealed class ActorSystemGetAsyncTests
{
    /// <summary>
    /// Non-ES actors are ephemeral — no persistence, cannot be rebuilt.
    /// GetAsync must return null for types that don't implement IEventSourcedActor.
    /// </summary>
    [Test]
    public async Task GetAsync_NonEventSourcedType_ReturnsNull()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<SimpleActor>(cmd =>
            cmd switch
            {
                NoOpCmd => new SimpleActor((NoOpCmd)cmd),
                _ => throw new InvalidOperationException(),
            }
        );

        var actor = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        var id = actor.Id;

        // Stop the actor — remove from memory
        await system.StopAsync(id);

        var result = await system.GetAsync<SimpleActor>(id);

        // Non-ES actors cannot be rebuilt after stop — return null
        await Assert.That(result).IsNull();
    }

    /// <summary>
    /// ES actors CAN be rebuilt from persisted events.
    /// </summary>
    [Test]
    public async Task GetAsync_EventSourcedType_AfterStop_RebuildsFromEvents()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<Counter>(
            cmd =>
                cmd switch
                {
                    CreateCounter c => new Counter(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new Counter()
        );

        // Create and persist events
        var actor = await system.CreateAsync<Counter>(new CreateCounter(0));
        var id = actor.Id;
        system.Send(id, new Increment(5));
        await Task.Delay(200); // Allow processing

        // Stop the actor — remove from registry
        await system.StopAsync(id);

        // GetAsync should rebuild from event store
        var rebuilt = await system.GetAsync<Counter>(id);

        await Assert.That(rebuilt).IsNotNull();
        await Assert.That(rebuilt!.Id).IsEqualTo(id);
    }
}
