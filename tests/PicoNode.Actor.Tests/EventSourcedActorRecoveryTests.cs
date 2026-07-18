using System.Collections.Concurrent;
using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Tests;

/// <summary>
/// Regression tests for the "poisoned actor" bug: after a persistence failure
/// during a normal (non-creation) message, Version and uncommitted events were
/// never rolled back. This left the actor permanently unable to persist and
/// leaked failed events into the next successful append.
///
/// Desired behavior: a failed append discards the uncommitted events and rolls
/// Version back to the last persisted version, so the actor recovers cleanly on
/// the next message and never reapplies a command whose caller already saw an
/// exception.
/// </summary>
public sealed class EventSourcedActorRecoveryTests
{
    /// <summary>
    /// Event store that fails the second AppendAsync call (transient), then
    /// succeeds. Call 1 = creation, call 2 = first normal message (fails),
    /// call 3+ = subsequent messages (succeed).
    /// </summary>
    private sealed class TransientFailingStore : IEventStore
    {
        private readonly ConcurrentDictionary<Guid, List<IDomainEvent>> _streams = new();
        private int _callCount;

        public ValueTask<ulong> AppendAsync(
            Guid actorId,
            ulong expectedVersion,
            IReadOnlyList<IDomainEvent> events
        )
        {
            var n = Interlocked.Increment(ref _callCount);
            if (n == 2)
                throw new InvalidOperationException("transient store failure");

            var stream = _streams.GetOrAdd(actorId, _ => []);
            var current = (ulong)stream.Count;
            if (current != expectedVersion)
                throw new ConcurrencyException(actorId, expectedVersion, current);

            stream.AddRange(events);
            return new ValueTask<ulong>((ulong)stream.Count);
        }

        public ValueTask<IReadOnlyList<IDomainEvent>> LoadAsync(Guid actorId)
        {
            if (_streams.TryGetValue(actorId, out var stream))
                return new ValueTask<IReadOnlyList<IDomainEvent>>(stream.AsReadOnly());
            return new ValueTask<IReadOnlyList<IDomainEvent>>(Array.Empty<IDomainEvent>());
        }
    }

    /// <summary>
    /// After a transient AppendAsync failure, the failed command's events must be
    /// discarded (its caller already received the exception). The next command
    /// must succeed and the actor's state must reflect ONLY the successful
    /// command — not the failed one.
    /// </summary>
    [Test]
    [Timeout(10000)]
    public async Task AppendFailure_DiscardsFailedEvents_NextMessageSucceedsCleanly()
    {
        var store = new TransientFailingStore();
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

        var actor = await system.CreateAsync<Counter>(new CreateCounter(0));
        var id = actor.Id;

        // First normal message — store fails transiently. Caller observes the failure.
        await Assert
            .That(async () => await system.AskAsync<object?>(id, new Increment(5)))
            .Throws<InvalidOperationException>();

        // Second normal message — store has recovered. Must succeed (actor not poisoned).
        await system.AskAsync<object?>(id, new Increment(3));

        // State must reflect ONLY the successful Increment(3), not the failed Increment(5).
        var value = await system.AskAsync<int>(id, new GetValue());
        await Assert.That(value).IsEqualTo(3);
    }

    /// <summary>
    /// After a failed append, the actor's Version must stay consistent with the
    /// last persisted version (no inflation from discarded events).
    /// </summary>
    [Test]
    [Timeout(10000)]
    public async Task AppendFailure_RollsBackVersion_ToLastPersisted()
    {
        var store = new TransientFailingStore();
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

        var actor = await system.CreateAsync<Counter>(new CreateCounter(0));
        var id = actor.Id;

        // Failed message (transient store failure on call 2)
        await Assert
            .That(async () => await system.AskAsync<object?>(id, new Increment(5)))
            .Throws<InvalidOperationException>();

        // Successful message advances Version by exactly 1 (from 1 -> 2),
        // proving the failed Increment(5) did not inflate Version.
        await system.AskAsync<object?>(id, new Increment(3));
        await Assert.That(actor.Version).IsEqualTo((ulong)2);
    }
}
