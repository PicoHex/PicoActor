namespace PicoActor.Tests;

/// <summary>Second first-event type so the enumerator has something to filter out.</summary>
public sealed record ProbeIgnored(int Seq) : IDomainEvent;

/// <summary>
/// Enumeration capability contract: recovery enumeration is I/O-shaped work
/// (per-stream gate + full scan), so the API is async — a synchronous
/// <c>SemaphoreSlim.Wait()</c> behind an async caller would block a pool thread.
/// </summary>
public sealed class InMemoryEventStoreEnumeratorTests
{
    [Test]
    public async Task ListAggregateIdsAsync_ReturnsOnlyStreamsWhoseFirstEventMatches(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var matching = Guid.CreateVersion7();
        var unrelated = Guid.CreateVersion7();
        await store.AppendAsync(matching, 0, [new ProbeCreated(1)]);
        await store.AppendAsync(unrelated, 0, [new ProbeIgnored(1)]);

        var ids = await store.ListAggregateIdsAsync(nameof(ProbeCreated));

        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(matching);
        await Assert.That(ids).DoesNotContain(unrelated);
    }

    [Test]
    public async Task ListAggregateIdsAsync_FirstEventTypeOnly_IgnoresLaterOccurrences(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var id = Guid.CreateVersion7();
        await store.AppendAsync(id, 0, [new ProbeIgnored(1)]);
        await store.AppendAsync(id, 1, [new ProbeCreated(2)]);

        var ids = await store.ListAggregateIdsAsync(nameof(ProbeCreated));

        // Only the FIRST event decides stream membership
        await Assert.That(ids.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ListAggregateIdsAsync_NoMatches_ReturnsEmpty(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();

        var ids = await store.ListAggregateIdsAsync(nameof(ProbeCreated));

        await Assert.That(ids.Count).IsEqualTo(0);
    }

    /// <summary>
    /// The runtime's recovery path must consume the async enumerator (no sync wait):
    /// FindAggregateIds stays awaitable and returns the matching ids.
    /// </summary>
    [Test]
    public async Task FindAggregateIds_UsesAsyncEnumerator(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var id = Guid.CreateVersion7();
        await store.AppendAsync(id, 0, [new ProbeCreated(7)]);
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });

        var ids = await system.FindAggregateIds(nameof(ProbeCreated), _ => true);

        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(id);
    }
}
