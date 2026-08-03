using PicoActor.Abs;

namespace PicoActor.Tests;

public sealed class ActorSystemStopAllTests
{
    [Test]
    public async Task StopAllAsync_StopsEveryActor_RegistryEmpty_SubsequentSendThrows()
    {
        var system = new ActorSystem(new InMemoryEventStore());
        system.Register<SimpleActor>(cmd =>
            cmd switch
            {
                NoOpCmd => new SimpleActor((NoOpCmd)cmd),
                _ => throw new InvalidOperationException(),
            }
        );

        var ids = new List<Guid>();
        for (var i = 0; i < 5; i++)
            ids.Add((await system.CreateAsync<SimpleActor>(new NoOpCmd())).Id);

        await system.StopAllAsync();

        // Registry is empty: messaging any stopped actor throws
        foreach (var id in ids)
            await Assert.That(() => system.Send(id, new NoOpCmd())).Throws<KeyNotFoundException>();

        // Idempotent: a second mass stop is a no-op
        await system.StopAllAsync();

        // System remains usable after the mass stop
        var fresh = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        await Assert.That(fresh.Id).IsNotEqualTo(Guid.Empty);
    }
}
