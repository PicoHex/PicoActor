using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

public sealed class ActorSystemConcurrencyTests
{
    /// <summary>
    /// Two concurrent GetAsync calls for the same ID must produce the same actor instance.
    /// The second caller must not create a duplicate that leaks.
    /// </summary>
    [Test]
    [Timeout(10000)]
    public async Task GetAsync_ConcurrentSameId_ReturnsSameInstance()
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

        // Create actor and persist state
        var original = await system.CreateAsync<Counter>(new CreateCounter(42));
        var id = original.Id;
        await system.StopAsync(id);

        // Issue two concurrent GetAsync calls
        Counter? a = null;
        Counter? b = null;

        var barrier = new Barrier(2);
        var t1 = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            a = await system.GetAsync<Counter>(id);
        });
        var t2 = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            b = await system.GetAsync<Counter>(id);
        });

        await Task.WhenAll(t1, t2);

        // Both must succeed
        await Assert.That(a).IsNotNull();
        await Assert.That(b).IsNotNull();

        // Both must reference the SAME instance
        await Assert.That(ReferenceEquals(a, b)).IsTrue();

        // Verify state was rebuilt correctly
        await Assert.That(a.Id).IsEqualTo(id);
    }
}
