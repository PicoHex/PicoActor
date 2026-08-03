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
        var system = new ActorSystem(new InMemoryEventStore());
        RegisterSimple(system);

        var actor = await system.CreateAsync<SimpleActor>(new NoOpCmd(), FixedId);

        await Assert.That(actor.Id).IsEqualTo(FixedId);
        var viaGet = await system.GetAsync<SimpleActor>(FixedId);
        await Assert.That(viaGet).IsNotNull();
    }

    [Test]
    public async Task CreateAsync_WithCallerSuppliedId_DuplicateId_ThrowsAndSystemUsable()
    {
        var system = new ActorSystem(new InMemoryEventStore());
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
        var systemA = new ActorSystem(store);
        RegisterCounter(systemA);
        await systemA.CreateAsync<Counter>(new CreateCounter(7), FixedId);

        // A fresh system over the same store must rebuild the same actor by id
        var systemB = new ActorSystem(store);
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
        var systemA = new ActorSystem(store);
        RegisterCounter(systemA);
        await systemA.CreateAsync<Counter>(new CreateCounter(1), FixedId);

        // "Second process" boot over the same store: the aggregate file already
        // exists — CreateAsync(id) must fail loudly, not silently overwrite.
        var systemB = new ActorSystem(store);
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
        var system = new ActorSystem(new InMemoryEventStore());
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
        var system = new ActorSystem(store);
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
}
