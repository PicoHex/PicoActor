namespace PicoActor.Tests;

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

        // "InMemory" claims no implementation: the store is whatever the host registered.
        container.AddPicoActorInMemoryStore();
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

        container.AddPicoActorInMemoryStore();
        container.AddPicoActor((ActorConfig?)null);
        container.Build();
        await using var scope = container.CreateScope();

        var store = (IEventStore)scope.GetService(typeof(IEventStore));
        await Assert.That(store).IsNotNull();
        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }

    /// <summary>
    /// Unsupported event store type still throws at registration time — the core registers no
    /// implementation, so an explicit type name other than "InMemory" can only be a mistake.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithUnsupportedStoreType_Throws()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var config = new ActorConfig { EventStore = new EventStoreConfig { Type = "Postgres" } };

        var ex = await Assert
            .That(() => container.AddPicoActor(config))
            .Throws<NotSupportedException>();

        // The message must name the ONLY supported shape: a store the host registered itself.
        await Assert.That(ex!.Message).Contains("'Postgres' is not supported");
        await Assert
            .That(ex.Message)
            .Contains(
                "Supported: whatever IEventStore is already registered "
                    + "(e.g. AddPicoActorInMemoryStore()); other stores are constructed explicitly."
            );
    }

    /// <summary>
    /// ActorConfig with no EventStore section should default to InMemory.
    /// </summary>
    [Test]
    public async Task AddPicoActor_WithNoEventStoreConfig_DefaultsToInMemory()
    {
        var container = new SvcContainer(autoConfigureFromGenerator: false);
        var config = new ActorConfig(); // No EventStore set

        container.AddPicoActorInMemoryStore();
        container.AddPicoActor(config);
        container.Build();
        await using var scope = container.CreateScope();

        var store = (IEventStore)scope.GetService(typeof(IEventStore));
        await Assert.That(store).IsNotNull();
        await Assert.That(store).IsTypeOf<InMemoryEventStore>();
    }
}
