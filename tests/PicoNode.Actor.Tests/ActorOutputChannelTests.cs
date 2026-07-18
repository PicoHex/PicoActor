using System.Threading.Channels;
using PicoNode.Actor;
using PicoNode.Actor.Abs;
using ActorBase = PicoNode.Actor.Abs.Actor;

namespace PicoNode.Actor.Tests;

/// <summary>
/// TDD: OutputChannel — Actor's outbound broadcast port.
/// Symmetric to Mailbox: Mailbox receives commands, OutputChannel sends events.
/// </summary>
public sealed class ActorOutputChannelTests
{
    /// <summary>Test actor that writes output events.</summary>
    private sealed class OutputTestActor : ActorBase
    {
        public OutputTestActor(NoOpCmd cmd)
            : base(cmd) { }

        public OutputTestActor() { }

        protected override ValueTask<object?> OnMessageAsync(ICommand command)
        {
            WriteOutput("text", "hello");
            WriteOutput("done");
            return default;
        }
    }

    [Test]
    public async Task OutputChannel_SubscriberReceivesEvents()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        var channel = Channel.CreateUnbounded<ActorOutputEvent>();
        var results = new List<ActorOutputEvent>();

        system.Register<OutputTestActor>(
            cmd =>
                cmd switch
                {
                    NoOpCmd c => new OutputTestActor(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new OutputTestActor()
        );

        var actor = await system.CreateAsync<OutputTestActor>(new NoOpCmd());

        // Set output channel on the actor
        actor.OutputWriter = channel.Writer;

        // Background reader
        _ = Task.Run(async () =>
        {
            await foreach (var e in channel.Reader.ReadAllAsync())
                results.Add(e);
        });

        // Send a command that produces output events
        system.Send(actor.Id, new NoOpCmd());
        await Task.Delay(200);
        channel.Writer.Complete();

        await Assert.That(results.Count).IsGreaterThanOrEqualTo(2);
        await Assert.That(results[0].Type).IsEqualTo("text");
        await Assert.That(results[1].Type).IsEqualTo("done");
    }

    [Test]
    public async Task OutputChannel_NoSubscriber_NoError()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(store);

        system.Register<OutputTestActor>(
            cmd =>
                cmd switch
                {
                    NoOpCmd c => new OutputTestActor(c),
                    _ => throw new InvalidOperationException(),
                },
            () => new OutputTestActor()
        );

        var actor = await system.CreateAsync<OutputTestActor>(new NoOpCmd());

        // No OutputWriter set — WriteOutput should be no-op, not crash
        system.Send(actor.Id, new NoOpCmd());
        await Task.Delay(100);

        // Should not throw
    }
}
