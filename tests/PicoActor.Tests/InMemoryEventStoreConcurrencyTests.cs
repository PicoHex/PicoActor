namespace PicoActor.Tests;

/// <summary>Test event — file-level so the Mediator generator can reference it.</summary>
public sealed record ProbeCreated(int Seq) : IDomainEvent;

/// <summary>
/// InMemoryEventStore thread-safety: ListAggregateIdsAsync reads stream state while
/// concurrent AppendAsync mutates the same List. The enumeration path must take
/// the per-stream gate like Append/Load/Peek do — otherwise the unsynchronized
/// List read can race AddRange's internal resize.
/// </summary>
public sealed class InMemoryEventStoreConcurrencyTests
{
    [Test]
    [Timeout(20000)]
    public async Task ListAggregateIds_ConcurrentAppend_NeverObservesTornState(
        CancellationToken cancellationToken = default
    )
    {
        const int appendCount = 1000;
        var store = new InMemoryEventStore();
        var hotId = Guid.CreateVersion7();
        var coldId = Guid.CreateVersion7();

        // Seed both streams so ListAggregateIdsAsync always has a first event to inspect.
        await store.AppendAsync(hotId, 0, [new ProbeCreated(0)]);
        await store.AppendAsync(coldId, 0, [new ProbeCreated(0)]);

        var errors = new ConcurrentBag<string>();
        var stopEnumeration = new CancellationTokenSource();

        var enumerator = Task.Run(
            async () =>
            {
                try
                {
                    while (!stopEnumeration.Token.IsCancellationRequested)
                    {
                        var ids = await store.ListAggregateIdsAsync(nameof(ProbeCreated));
                        if (ids.Count != 2 || !ids.Contains(hotId) || !ids.Contains(coldId))
                            errors.Add(
                                $"unexpected enumeration result: [{string.Join(", ", ids)}]"
                            );
                    }
                }
                catch (Exception ex)
                {
                    errors.Add($"enumeration faulted: {ex.GetType().Name}: {ex.Message}");
                }
            },
            CancellationToken.None
        );

        // Single sequential writer on the hot stream — correct expectedVersion,
        // so no OCC conflicts; the pressure is enumeration-vs-AddRange only.
        await Task.Run(async () =>
        {
            for (var i = 1; i <= appendCount; i++)
                await store.AppendAsync(hotId, (ulong)i, [new ProbeCreated(i)]);
        });

        stopEnumeration.Cancel();
        await enumerator;

        // All writes landed exactly once.
        var stream = await store.LoadAsync(hotId);
        await Assert.That(stream.Count).IsEqualTo(appendCount + 1);

        if (!errors.IsEmpty)
            Assert.Fail(
                $"ListAggregateIdsAsync raced concurrent appends ({errors.Count} failures, first: {errors.First()})"
            );
    }
}
