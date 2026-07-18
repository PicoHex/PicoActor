using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

public sealed class ActorSystemCreationTests
{
    /// <summary>
    /// When the factory's constructor throws, the exception must propagate.
    /// The system must remain usable — subsequent CreateAsync calls must succeed.
    /// </summary>
    [Test]
    public async Task CreateAsync_WhenFactoryThrows_PropagatesException()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<ThrowingActor>(cmd =>
            cmd switch
            {
                Explode => new ThrowingActor((Explode)cmd),
                _ => throw new InvalidOperationException(),
            }
        );

        // Factory constructor throws — exception must propagate
        await Assert
            .That(async () => await system.CreateAsync<ThrowingActor>(new Explode()))
            .Throws<InvalidOperationException>();
    }

    /// <summary>
    /// After a failed CreateAsync, the system must still be functional.
    /// This verifies that no resources are leaked (zombie actors, unclosed Channels).
    /// </summary>
    [Test]
    public async Task CreateAsync_AfterFailedCreation_SystemRemainsUsable()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<ThrowingActor>(cmd =>
            cmd switch
            {
                Explode => new ThrowingActor((Explode)cmd),
                _ => throw new InvalidOperationException(),
            }
        );

        system.Register<SimpleActor>(cmd =>
            cmd switch
            {
                NoOpCmd => new SimpleActor((NoOpCmd)cmd),
                _ => throw new InvalidOperationException(),
            }
        );

        // First creation fails
        try
        {
            await system.CreateAsync<ThrowingActor>(new Explode());
        }
        catch (InvalidOperationException)
        { /* expected */
        }

        // Second creation must succeed — no resource leak from first
        var actor = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        await Assert.That(actor).IsNotNull();
        await Assert.That(actor.Id).IsNotEqualTo(Guid.Empty);

        await system.StopAsync(actor.Id);
    }
}
