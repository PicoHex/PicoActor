// ═══════════════════════════════════════════════════════════════
// PicoNode.Actor Sample — Event-Sourced Counter
//
// Demonstrates:
//   - ActorSystem + InMemoryEventStore setup
//   - Register with create/rebuild factories
//   - CreateAsync, Send (fire-and-forget), AskAsync (request-reply)
//   - Multi-event scenario (Reset → CounterReset event)
//   - OutputChannel subscription
//   - StopAsync + GetAsync (event-sourced rebuild from persistence)
//   - Idempotent StopAsync
// ═══════════════════════════════════════════════════════════════

Console.WriteLine("=== PicoNode.Actor Sample: Event-Sourced Counter ===\n");

// ── Setup: ActorSystem with InMemoryEventStore ─────────────────
var store = new InMemoryEventStore();
var system = new ActorSystem(store);

system.Register<Counter>(
    createFactory: cmd => cmd switch
    {
        CreateCounter c => new Counter(c),
        _ => throw new InvalidOperationException($"Unexpected creation command: {cmd.GetType().Name}"),
    },
    rebuildFactory: () => new Counter()
);

// ── CreateAsync ────────────────────────────────────────────────
var counter = await system.CreateAsync<Counter>(new CreateCounter(10));
Console.WriteLine($"[CreateAsync] Counter created: Id={counter.Id}");
Console.WriteLine($"[AskAsync]    GetValue → {await system.AskAsync<int>(counter.Id, new GetValue())}");

// ── Send (fire-and-forget) ─────────────────────────────────────
system.Send(counter.Id, new Increment(5));
system.Send(counter.Id, new Increment(3));
await Task.Delay(100); // yield to let mailbox process
Console.WriteLine($"[Send+Ask]    After +5, +3 → {await system.AskAsync<int>(counter.Id, new GetValue())}");

system.Send(counter.Id, new Decrement(2));
await Task.Delay(100);
Console.WriteLine($"[Send+Ask]    After -2 → {await system.AskAsync<int>(counter.Id, new GetValue())}");

// ── Multi-event scenario (Reset) ───────────────────────────────
system.Send(counter.Id, new Reset(100));
await Task.Delay(100);
Console.WriteLine($"[MultiEvent]  After Reset(100) → {await system.AskAsync<int>(counter.Id, new GetValue())}");
Console.WriteLine($"              Version: {counter.Version} (5 events applied)");

// ── Subscribe to OutputChannel ─────────────────────────────────
Console.WriteLine("\n── OutputChannel events (live) ──");
var outChannel = System.Threading.Channels.Channel.CreateUnbounded<ActorOutputEvent>();
counter.OutputWriter = outChannel.Writer;

system.Send(counter.Id, new Increment(42));
system.Send(counter.Id, new Decrement(7));
await Task.Delay(200); // yield to let mailbox process

while (outChannel.Reader.TryRead(out var evt))
    Console.WriteLine($"  OutputEvent: {evt.Type}, Data={evt.Data}");

var finalValue = await system.AskAsync<int>(counter.Id, new GetValue());
Console.WriteLine($"[Final]       GetValue → {finalValue}");

// ── StopAsync & GetAsync (event-sourced rebuild) ───────────────
Console.WriteLine("\n── Stop & Rebuild (event-sourced recovery) ──");
var oldId = counter.Id;
await system.StopAsync(oldId);
Console.WriteLine($"[StopAsync]   Actor {oldId} stopped");

// Rebuild from persisted events
var rebuilt = await system.GetAsync<Counter>(oldId);
Console.WriteLine($"[GetAsync]    Actor {oldId} rebuilt from event stream");
Console.WriteLine($"[AskAsync]    Rebuilt GetValue → {await system.AskAsync<int>(rebuilt!.Id, new GetValue())}");
Console.WriteLine($"              Version: {rebuilt.Version} (7 events replayed)");
Console.WriteLine($"              Same Id: {rebuilt.Id == oldId}");

// ── Idempotent StopAsync ───────────────────────────────────────
await system.StopAsync(rebuilt.Id);
await system.StopAsync(rebuilt.Id); // second call: no-op, no exception
Console.WriteLine("[StopAsync]   Second StopAsync is idempotent (no-op)");

// ── GetAsync returns null after Stop (non-ES) ──────────────────
// Note: Counter is an EventSourcedActor, so GetAsync will rebuild it again.
// This is by design — ES actors are persistent.
var reRebuilt = await system.GetAsync<Counter>(oldId);
Console.WriteLine(
    $"[GetAsync]    Re-rebuild (ES actor persists) → {(reRebuilt is not null ? "found" : "null (non-ES)")}"
);

Console.WriteLine("\n=== Done ===");

// ═══════════════════════════════════════════════════════════════
// Domain types (below top-level statements)
// ═══════════════════════════════════════════════════════════════

// ── Commands ───────────────────────────────────────────────────
public sealed record CreateCounter(int InitialValue) : ICommand;
public sealed record Increment(int Delta) : ICommand;
public sealed record Decrement(int Delta) : ICommand;
public sealed record Reset(int NewValue) : ICommand;
public sealed record GetValue : ICommand;

// ── Domain Events ──────────────────────────────────────────────
public sealed record CounterCreated(int InitialValue) : IDomainEvent;
public sealed record CounterIncremented(int Delta) : IDomainEvent;
public sealed record CounterDecremented(int Delta) : IDomainEvent;
public sealed record CounterReset(int OldValue, int NewValue) : IDomainEvent;

// ── Event-Sourced Counter Actor ───────────────────────────────
public sealed class Counter : EventSourcedActor
{
    private int _value;

    /// <summary>Creation path — atomic initialization from CreateCounter.</summary>
    public Counter(CreateCounter cmd) : base(cmd) { }

    /// <summary>Rebuild path — required for GetAsync event-sourced recovery.</summary>
    public Counter() { }

    /// <summary>
    /// Dispatch commands. RaiseEvent records events but does NOT mutate state.
    /// After this returns, the framework persists events to IEventStore,
    /// then calls Mutate to apply state changes.
    /// </summary>
    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case CreateCounter c:
                RaiseEvent(new CounterCreated(c.InitialValue));
                WriteOutput("CounterCreated", data: $"InitialValue={c.InitialValue}");
                return default;

            case Increment i when i.Delta <= 0:
                throw new ArgumentException("Delta must be positive", nameof(i));
            case Increment i:
                RaiseEvent(new CounterIncremented(i.Delta));
                WriteOutput("Incremented", data: $"Delta={i.Delta}");
                return default;

            case Decrement d when d.Delta <= 0:
                throw new ArgumentException("Delta must be positive", nameof(d));
            case Decrement d:
                RaiseEvent(new CounterDecremented(d.Delta));
                WriteOutput("Decremented", data: $"Delta={d.Delta}");
                return default;

            case Reset r:
                // Multi-event in one message
                RaiseEvent(new CounterReset(_value, r.NewValue));
                WriteOutput("CounterReset", data: $"Old={_value},New={r.NewValue}");
                return default;

            case GetValue:
                return new ValueTask<object?>(_value);
        }

        return default;
    }

    /// <summary>
    /// Mutate is a pure function — called after persistence succeeds
    /// (normal flow) and during ReplayEvents (recovery).
    /// No validation, no I/O, no side effects.
    /// </summary>
    protected override void Mutate(IDomainEvent @event)
    {
        switch (@event)
        {
            case CounterCreated e:  _value = e.InitialValue;  break;
            case CounterIncremented e: _value += e.Delta;     break;
            case CounterDecremented e: _value -= e.Delta;     break;
            case CounterReset e:       _value = e.NewValue;   break;
        }
    }
}
