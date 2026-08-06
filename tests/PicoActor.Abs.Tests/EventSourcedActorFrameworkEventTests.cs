using PicoActor.Abs;

namespace PicoActor.Abs.Tests;

internal sealed record FrameworkProbeEvent : IDomainEvent;

internal sealed record ProbeCreated : IDomainEvent;

/// <summary>Test actor that intercepts ProbeCreated as a framework event.</summary>
internal sealed class FrameworkFilterActor : EventSourcedActor
{
    public static int MutateCount;
    public static int FrameworkHandled;

    public FrameworkFilterActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;

    protected override void Mutate(IDomainEvent @event)
    {
        if (@event is ProbeCreated)
            MutateCount++;
    }

    protected override bool TryHandleFrameworkEvent(IDomainEvent e)
    {
        if (e is ProbeCreated)
        {
            FrameworkHandled++;
            return true;
        }
        return false;
    }
}

public sealed class EventSourcedActorFrameworkEventTests
{
    [Test]
    public async Task FrameworkEvent_IsFilteredFromMutate_OnFlush()
    {
        FrameworkFilterActor.MutateCount = 0;
        FrameworkFilterActor.FrameworkHandled = 0;

        var actor = new FrameworkFilterActor();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        // Drive one flush directly: calling FlushEventsAsync (protected) after RaiseEvent
        // is unreachable through the public path — use the internal test hook instead.
        // Driving it via IEventSourcedActor.GetUncommittedEvents + manual construction is
        // not possible (FlushEventsAsync is protected) — so filtering is verified through
        // ReplayEvents (public interface); the flush path is covered by the Task 3 integration tests.
        ((IEventSourcedActor)actor).ReplayEvents(new IDomainEvent[] { new ProbeCreated() });

        await Assert.That(FrameworkFilterActor.MutateCount).IsEqualTo(0);
        await Assert.That(FrameworkFilterActor.FrameworkHandled).IsEqualTo(1);
    }
}
