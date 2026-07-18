using PicoNode.Actor.Abs;
using ActorBase = PicoNode.Actor.Abs.Actor;

namespace PicoNode.Actor.Tests;

// ═══════════════════════════════════════════════════════════
// Test actors
// ═══════════════════════════════════════════════════════════

internal sealed record CreateCounter(int InitialValue) : ICommand;

internal sealed record Increment(int Delta) : ICommand;

internal sealed record GetValue : ICommand;

internal sealed record CounterCreated(int InitialValue) : IDomainEvent;

internal sealed record CounterIncremented(int Delta) : IDomainEvent;

internal sealed class Counter : EventSourcedActor
{
    private int _value;

    public Counter(CreateCounter cmd)
        : base(cmd) { }

    public Counter() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case CreateCounter c:
                RaiseEvent(new CounterCreated(c.InitialValue));
                return default;
            case Increment i:
                RaiseEvent(new CounterIncremented(i.Delta));
                return default;
            case GetValue:
                return new ValueTask<object?>(_value);
            default:
                return default;
        }
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case CounterCreated e:
                _value = e.InitialValue;
                break;
            case CounterIncremented e:
                _value += e.Delta;
                break;
        }
    }
}

/// <summary>Non-ES actor — ephemeral, cannot be rebuilt.</summary>
internal sealed record NoOpCmd : ICommand;

internal sealed class SimpleActor : ActorBase
{
    public SimpleActor(NoOpCmd cmd)
        : base(cmd) { }

    public SimpleActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) => default;
}

/// <summary>Actor whose constructor always throws.</summary>
internal sealed record Explode : ICommand;

internal sealed class ThrowingActor : ActorBase
{
    public ThrowingActor(Explode cmd)
        : base(cmd) { }

    public ThrowingActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        throw new InvalidOperationException("boom in constructor");
}
