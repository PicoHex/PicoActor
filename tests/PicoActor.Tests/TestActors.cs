namespace PicoActor.Tests;

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

/// <summary>Reports the actor-loop thread id — used to verify Ask continuations
/// do not run inline on the completing actor's loop thread.</summary>
internal sealed record GetLoopThreadId : ICommand;

/// <summary>Blocks the actor loop thread until the gate is set — used to prove
/// Ask continuations never run inline on the loop thread: after the loop
/// completes the ask's TCS it immediately blocks itself, so the queued
/// continuation provably cannot run on the loop thread (a blocked thread is
/// never handed pool work).</summary>
internal sealed record BlockLoopCmd(ManualResetEventSlim Gate) : ICommand;

internal sealed class ThreadProbeActor : ActorBase
{
    public ThreadProbeActor(NoOpCmd cmd)
        : base(cmd) { }

    public ThreadProbeActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        command switch
        {
            GetLoopThreadId => new ValueTask<object?>(Environment.CurrentManagedThreadId),
            BlockLoopCmd b => Block(b.Gate),
            _ => default,
        };

    private static ValueTask<object?> Block(ManualResetEventSlim gate)
    {
        gate.Wait();
        return default;
    }
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

// ═══════════════════════════════════════════════════════════
// Saga test actor
// ═══════════════════════════════════════════════════════════

internal sealed record StartSaga(string Name) : ICommand;

internal sealed record GetSagaStep : ICommand;

internal sealed record SagaStep1Started(string Name) : IDomainEvent;

internal sealed record SagaStep2Done : IDomainEvent;

internal sealed class TestSaga : SagaActor
{
    private int _step;
    private string _name = "";

    public TestSaga() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        if (command is StartSaga s)
        {
            if (_step < 1)
            {
                RaiseEvent(new SagaStep1Started(s.Name));
                _name = s.Name;
            }
            if (_step < 2)
                RaiseEvent(new SagaStep2Done());
            // Unconditional call is intentional: an interrupted saga whose steps are all
            // done but has no terminal event converges to completion on resume; repeated
            // commands after completion are refused by the terminal guard (persisted in
            // separate batches, so no duplicate completion event is appended)
            MarkComplete(_name);
            return _name;
        }
        if (command is GetSagaStep)
            return _step;
        return null;
    }

    protected override async ValueTask ResumeAsync()
    {
        await OnMessageAsync(new StartSaga(_name));
    }

    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case SagaStep1Started e:
                _step = 1;
                _name = e.Name;
                break;
            case SagaStep2Done:
                _step = 2;
                break;
        }
    }
}
