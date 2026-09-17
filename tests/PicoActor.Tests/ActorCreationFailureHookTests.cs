namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A creation-command failure must leave a dead actor: the consumption loop's
// OnReadyAsync lifecycle hook must NOT run (it is for live actors only). Regression:
// the constructor catch released the ready gate without marking the actor discarded,
// so the loop resumed and ran the hook on the dead instance.
// ═══════════════════════════════════════════════════════════

internal sealed record CreationHookCmd : ICommand;

internal sealed class CreationHookProbeActor : EventSourcedActor
{
    public static int ReadyRuns;

    public CreationHookProbeActor(CreationHookCmd cmd)
        : base(cmd) { }

    public CreationHookProbeActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        throw new InvalidOperationException("creation boom");

    protected override void Mutate(IDomainEvent @event) { }

    protected override ValueTask OnReadyAsync()
    {
        Interlocked.Increment(ref ReadyRuns);
        return base.OnReadyAsync();
    }
}

public sealed class ActorCreationFailureHookTests
{
    [Test]
    public async Task CreationFailure_DoesNotRunOnReadyAsync()
    {
        CreationHookProbeActor.ReadyRuns = 0;
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<CreationHookProbeActor>(_ => new CreationHookProbeActor(
            new CreationHookCmd()
        ));

        await Assert
            .That(async () =>
                await system.CreateAsync<CreationHookProbeActor>(new CreationHookCmd())
            )
            .Throws<InvalidOperationException>();

        // Give the gated loop time to resume (and run the hook) if it were going to.
        await Task.Delay(300);

        await Assert.That(CreationHookProbeActor.ReadyRuns).IsEqualTo(0);
    }
}
