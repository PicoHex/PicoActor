using PicoActor.Abs;

namespace PicoActor.Tests;

/// <summary>
/// Tests for IActorSystem.FindAggregateIds — event-store enumeration for
/// recovery paths (content adoption, singleton scan).
/// </summary>
public sealed class ActorSystemFindAggregateIdsTests
{
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
    public async Task FindAggregateIds_MatchesFirstEvent_ReturnsIds()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        RegisterCounter(system);
        var a = await system.CreateAsync<Counter>(new CreateCounter(1));
        var b = await system.CreateAsync<Counter>(new CreateCounter(2));

        var found = await system.FindAggregateIds(
            nameof(CounterCreated),
            e => e is CounterCreated c && c.InitialValue == 2
        );

        await Assert.That(found.Count).IsEqualTo(1);
        await Assert.That(found[0]).IsEqualTo(b.Id);
        // The non-matching aggregate stays excluded
        await Assert.That(found).DoesNotContain(a.Id);
    }

    [Test]
    public async Task FindAggregateIds_StoreWithoutEnumeration_ReturnsEmpty()
    {
        // Store implementing only IEventStore — no enumeration capability.
        // FindAggregateIds must return empty, not throw.
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new NonEnumeratingStore() }
        );
        RegisterCounter(system);
        var a = await system.CreateAsync<Counter>(new CreateCounter(1));

        var found = await system.FindAggregateIds(nameof(CounterCreated), _ => true);

        await Assert.That(found.Count).IsEqualTo(0);
        // Actor still works — enumeration absence never affects runtime
        var value = await system.AskAsync<int>(a.Id, new GetValue());
        await Assert.That(value).IsEqualTo(1);
    }

    /// <summary>Minimal store without IEventStoreEnumerator.</summary>
    private sealed class NonEnumeratingStore : IEventStore
    {
        private readonly InMemoryEventStore _inner = new();

        public ValueTask<ulong> AppendAsync(
            Guid actorId,
            ulong expectedVersion,
            IReadOnlyList<IDomainEvent> events
        ) => _inner.AppendAsync(actorId, expectedVersion, events);

        public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId) =>
            _inner.LoadAsync(actorId);
    }
}
