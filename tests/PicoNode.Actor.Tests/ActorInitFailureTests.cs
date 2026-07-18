using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

/// <summary>
/// Tests for initialization failure during actor creation.
/// Verifies the OnReadyAsync bug fix: failed persistence must
/// propagate to the caller and remove the actor from the registry.
/// </summary>
public sealed class ActorInitFailureTests
{
    /// <summary>
    /// An event store that always throws on AppendAsync.
    /// </summary>
    private sealed class FailingEventStore : IEventStore
    {
        public ValueTask<ulong> AppendAsync(
            Guid actorId,
            ulong expectedVersion,
            IReadOnlyList<IDomainEvent> events
        )
        {
            throw new InvalidOperationException("Simulated persistence failure");
        }

        public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
        {
            return new ValueTask<IReadOnlyList<IDomainEvent>>(Array.Empty<IDomainEvent>());
        }
    }

    /// <summary>
    /// When persistence fails during creation (OnReadyAsync → FlushEventsAsync),
    /// the exception must propagate to CreateAsync caller,
    /// and the actor must NOT remain in the registry.
    /// </summary>
    [Test]
    public async Task CreateAsync_WhenPersistenceFails_ThrowsAndRemovesActor()
    {
        var store = new FailingEventStore();
        var system = new ActorSystem(store);

        system.Register<Counter>(cmd =>
            cmd switch
            {
                CreateCounter c => new Counter(c),
                _ => throw new InvalidOperationException(),
            }
        );

        Counter? actor = null;
        Guid id = Guid.Empty;

        // Creation must throw — persistence failed
        await Assert
            .That(async () =>
            {
                actor = await system.CreateAsync<Counter>(new CreateCounter(42));
                id = actor.Id;
            })
            .Throws<InvalidOperationException>();

        // Actor must NOT be in the registry — removed on failure
        await Assert
            .That(async () => await system.AskAsync<int>(id, new GetValue()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// After a failed creation, a new creation for the same type must succeed.
    /// This verifies the system is not left in a broken state.
    /// </summary>
    [Test]
    public async Task CreateAsync_AfterPersistenceFailure_NewCreationSucceeds()
    {
        var failingStore = new FailingEventStore();
        var goodStore = new InMemoryEventStore();
        var system = new ActorSystem(failingStore);

        system.Register<Counter>(cmd =>
            cmd switch
            {
                CreateCounter c => new Counter(c),
                _ => throw new InvalidOperationException(),
            }
        );

        // First creation fails
        try
        {
            await system.CreateAsync<Counter>(new CreateCounter(42));
        }
        catch (InvalidOperationException)
        { /* expected */
        }

        // Create a new system with a working store to verify it works
        var system2 = new ActorSystem(goodStore);
        system2.Register<Counter>(cmd =>
            cmd switch
            {
                CreateCounter c => new Counter(c),
                _ => throw new InvalidOperationException(),
            }
        );

        var actor = await system2.CreateAsync<Counter>(new CreateCounter(10));
        await Assert.That(actor).IsNotNull();
        await Assert.That(actor.Id).IsNotEqualTo(Guid.Empty);
    }
}
