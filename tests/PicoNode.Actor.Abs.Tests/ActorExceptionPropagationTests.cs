using PicoNode.Actor.Abs;

namespace PicoNode.Actor.Abs.Tests;

internal sealed record TestCommand : ICommand;

/// <summary>Actor that always throws — for testing exception propagation.</summary>
internal sealed class ThrowingActor : Actor
{
    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        throw new InvalidOperationException("boom");
}

public sealed class ActorExceptionPropagationTests
{
    /// <summary>
    /// BUG: When OnMessageAsync throws, the TCS must be set to faulted.
    /// Otherwise AskAsync callers hang forever.
    /// </summary>
    [Test]
    [Timeout(5000)]
    public async Task Post_when_OnMessageAsync_throws_sets_Tcs_to_faulted()
    {
        var actor = new ThrowingActor();
        // Simulate what ActorSystem does: assign Id, then release the gate
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        var tcs = new TaskCompletionSource<object?>();

        actor.Post(new Envelope { Command = new TestCommand(), Tcs = tcs });

        // Must NOT hang — must receive the exception
        await Assert.That(async () => await tcs.Task).Throws<InvalidOperationException>();
    }

    /// <summary>
    /// BUG: Send (fire-and-forget, no TCS) silently swallowed every exception —
    /// envelope.Tcs?.TrySetException(ex) is a no-op when Tcs is null, with no
    /// log and no observable signal. A failed command vanished without a trace.
    /// Desired: an unhandled-error handler is invoked so the system can log it.
    /// </summary>
    [Test]
    [Timeout(5000)]
    public async Task Send_when_OnMessageAsync_throws_invokes_UnhandledErrorHandler()
    {
        var actor = new ThrowingActor();
        actor.Id = Guid.CreateVersion7();
        actor.SignalReady();

        Exception? caught = null;
        ICommand? failedCmd = null;
        actor.UnhandledErrorHandler = (ex, cmd) =>
        {
            caught = ex;
            failedCmd = cmd;
        };

        // Send = fire-and-forget (no TCS)
        actor.Post(new Envelope { Command = new TestCommand() });

        // Allow the async consumption loop to process the message
        await Task.Delay(300);

        await Assert.That(caught).IsNotNull();
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
        await Assert.That(failedCmd).IsTypeOf<TestCommand>();
    }
}
