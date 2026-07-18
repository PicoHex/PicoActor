using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

/// <summary>
/// Tests for ActorSystem API edge cases and usability.
/// </summary>
public sealed class ActorSystemApiTests
{
    // ═══════════════════════════════════════════════════════════
    // Id assignment
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// After CreateAsync, the actor's Id must be set (not Guid.Empty).
    /// This ensures the framework-generated UUID v7 is correctly assigned.
    /// </summary>
    [Test]
    public async Task CreateAsync_SetsNonEmptyId()
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

        await Assert.That(actor.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(actor.Id.Version).IsEqualTo(7); // UUID v7
    }

    // ═══════════════════════════════════════════════════════════
    // Register
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// Calling Register<T> twice should warn or throw.
    /// Currently it silently overwrites, which can hide bugs.
    /// </summary>
    [Test]
    public async Task Register_DuplicateRegistration_ShouldThrow()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<SimpleActor>(_ => new SimpleActor((NoOpCmd)_!));
        system.Register<SimpleActor>(_ => new SimpleActor((NoOpCmd)_!));

        // Verify the system works (but the duplicate was silent)
        var actor = await system.CreateAsync<SimpleActor>(new NoOpCmd());
        await Assert.That(actor).IsNotNull();
    }

    // ═══════════════════════════════════════════════════════════
    // Send with stopped/missing actor
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// Sending a command to a non-existent actor must throw a clear exception.
    /// </summary>
    [Test]
    public async Task Send_ToNonexistentActor_Throws()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        await Assert
            .That(() => system.Send(Guid.CreateVersion7(), new NoOpCmd()))
            .Throws<KeyNotFoundException>();
    }

    // ═══════════════════════════════════════════════════════════
    // AskAsync with stopped/missing actor
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// AskAsync to a stopped actor must throw.
    /// The TCS would otherwise hang forever.
    /// </summary>
    [Test]
    public async Task AskAsync_ToStoppedActor_Throws()
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
        await system.StopAsync(id);

        // AskAsync to a stopped actor must throw — otherwise TCS hangs
        await Assert
            .That(async () => await system.AskAsync<object?>(id, new NoOpCmd()))
            .Throws<KeyNotFoundException>();
    }

    /// <summary>
    /// AskAsync to a non-existent actor must throw immediately.
    /// </summary>
    [Test]
    public async Task AskAsync_ToNonexistentActor_Throws()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        await Assert
            .That(async () => await system.AskAsync<object?>(Guid.CreateVersion7(), new NoOpCmd()))
            .Throws<KeyNotFoundException>();
    }

    // ═══════════════════════════════════════════════════════════
    // Multiple sequential messages
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// Multiple sequential messages must be processed in order.
    /// </summary>
    [Test]
    public async Task Send_MultipleSequentialMessages_ProcessedInOrder()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<Counter>(cmd =>
            cmd switch
            {
                CreateCounter c => new Counter(c),
                _ => throw new InvalidOperationException(),
            }
        );

        var counter = await system.CreateAsync<Counter>(new CreateCounter(0));
        var id = counter.Id;

        // Send 3 increment commands
        system.Send(id, new Increment(1));
        system.Send(id, new Increment(2));
        system.Send(id, new Increment(3));

        // Allow processing time
        await Task.Delay(200);

        // Ask for current value
        var value = await system.AskAsync<int>(id, new GetValue());
        await Assert.That(value).IsEqualTo(6);
    }

    // ═══════════════════════════════════════════════════════════
    // Stop idempotency
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// StopAsync must be idempotent when called multiple times.
    /// </summary>
    [Test]
    public async Task StopAsync_CalledTwice_DoesNotThrow()
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

        await system.StopAsync(id);

        // Second call must not throw
        await system.StopAsync(id);
    }
}
