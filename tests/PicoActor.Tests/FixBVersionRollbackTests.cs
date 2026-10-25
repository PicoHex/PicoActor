namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// P6 — the two rollback helpers must agree on what "roll
//      back" means. Subtracting a count from Version is not
//      enough when the handler cleared its own staged events
//      mid-dispatch (CommitEvents): the count is 0 while the
//      Version is already inflated, so the next flush computes
//      a phantom expectedVersion and dies with ConcurrencyException.
// ═══════════════════════════════════════════════════════════

internal sealed record FixBVersionCreate : ICommand;

internal sealed record FixBLeakThenThrow : ICommand;

internal sealed record FixBLeakOk : ICommand;

internal sealed record FixBLeakEvent(int Value) : IDomainEvent;

/// <summary>
/// Actor whose throwing dispatch has already "cleaned up" its staged events with
/// <c>CommitEvents()</c> — the misuse the interface doc now calls out. The event list is
/// empty again, so a count-based rollback sees nothing to undo, while Version stays inflated.
/// </summary>
internal sealed class FixBCommitThenThrowActor : EventSourcedActor
{
    public FixBCommitThenThrowActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixBLeakThenThrow:
                RaiseEvent(new FixBLeakEvent(1));
                ((IEventSourcedActor)this).CommitEvents(); // nothing was persisted
                throw new InvalidOperationException("dispatch boom");
            case FixBLeakOk:
                RaiseEvent(new FixBLeakEvent(2));
                return new ValueTask<object?>(true);
            default:
                return default;
        }
    }

    protected override void Mutate(IDomainEvent @event) { }
}

public sealed class FixBVersionRollbackTests
{
    /// <summary>
    /// After a dispatch that throws — even one that cleared its own uncommitted events
    /// first — the next command must persist normally. Version has to be restored from the
    /// dispatch snapshot, not derived from the (now empty) uncommitted list.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task DispatchThrowAfterCommitEvents_NextCommandPersistsNormally(
        CancellationToken cancellationToken = default
    )
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixBCommitThenThrowActor>(_ => new FixBCommitThenThrowActor());

        var actor = await system.CreateAsync<FixBCommitThenThrowActor>(new FixBVersionCreate());

        // The dispatch raises one event, clears the list, then throws: nothing was persisted.
        await Assert
            .That(async () => await system.AskAsync<bool>(actor.Id, new FixBLeakThenThrow()))
            .Throws<InvalidOperationException>();

        await Assert.That(await store.LoadAsync(actor.Id)).IsEmpty();
        await Assert.That(actor.Version).IsEqualTo(0UL);

        // The next command appends on the last PERSISTED baseline — a phantom
        // expectedVersion would make every later command fail with ConcurrencyException.
        await Assert.That(await system.AskAsync<bool>(actor.Id, new FixBLeakOk())).IsTrue();

        var stream = await store.LoadAsync(actor.Id);
        await Assert.That(stream.Count).IsEqualTo(1);
        await Assert.That(stream[0]).IsTypeOf<FixBLeakEvent>();
        await Assert.That(actor.Version).IsEqualTo(1UL);
    }
}
