using PicoDI;
using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

public sealed class ActorConfigTests
{
    /// <summary>
    /// ActorConfig with InMemory type should create InMemoryEventStore.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithInMemoryConfig_UsesInMemoryStore()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var config = new ActorConfig { EventStore = new EventStoreConfig { Type = "InMemory" } };

        container.AddPicoActor(config);
        container.Build();
        await using var scope = container.CreateScope();

        var store = (IEventStore)scope.GetService(typeof(IEventStore));
        await Assert.That(store).IsNotNull();
        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }

    /// <summary>
    /// Null config should default to InMemoryEventStore.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithNullConfig_DefaultsToInMemory()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);

        container.AddPicoActor((ActorConfig?)null);
        container.Build();
        await using var scope = container.CreateScope();

        var store = (IEventStore)scope.GetService(typeof(IEventStore));
        await Assert.That(store).IsNotNull();
        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }

    /// <summary>
    /// Unsupported event store type should throw at registration time.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithUnsupportedStoreType_Throws()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var config = new ActorConfig { EventStore = new EventStoreConfig { Type = "Postgres" } };

        await Assert.That(() => container.AddPicoActor(config)).Throws<NotSupportedException>();
    }

    /// <summary>
    /// ActorConfig with no EventStore section should default to InMemory.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithNoEventStoreConfig_DefaultsToInMemory()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var config = new ActorConfig(); // No EventStore set

        container.AddPicoActor(config);
        container.Build();
        await using var scope = container.CreateScope();

        var store = (IEventStore)scope.GetService(typeof(IEventStore));
        await Assert.That(store).IsNotNull();
        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }
}
