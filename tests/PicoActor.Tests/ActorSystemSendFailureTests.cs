// TUnit0055: the no-logger diagnostic tests deliberately redirect Console.Error to capture
// the stderr fallback; the finally block restores it.
// [NotInParallel]: Console.Error is process-global — the redirecting tests must not run
// concurrently with each other, or one test's restore races the other's capture.
// The throwing sink must also stay scoped to PicoActor diagnostics: a blanket throw on a
// process-global writer kills the whole test process from the runner's finalizer-thread
// diagnostics (0xE0434352 + a silently truncated test count) — see ThrowingTextWriter.
#pragma warning disable TUnit0055

namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// Fire-and-forget delivery must not fail silently:
//  - a failing Send without a logger writes a stderr diagnostic (no silent swallow)
//  - a Send into an already-closed mailbox throws instead of dropping the command
// Console.Error is process-global → this class must not run in parallel.
// ═══════════════════════════════════════════════════════════

internal sealed record SendFailureProbe : ICommand;

internal sealed record SendFailureBoom : ICommand;

internal sealed class SendFailureProbeActor : Actor
{
    public SendFailureProbeActor(SendFailureProbe cmd)
        : base(cmd) { }

    public SendFailureProbeActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        command is SendFailureBoom ? throw new InvalidOperationException("send boom") : default;
}

[NotInParallel]
public sealed class ActorSystemSendFailureTests
{
    private static ActorSystem CreateSystem()
    {
        var system = new ActorSystem(
            new ActorSystemOptions { EventStore = new InMemoryEventStore() }
        );
        system.Register<SendFailureProbeActor>(cmd => new SendFailureProbeActor(
            (SendFailureProbe)cmd
        ));
        return system;
    }

    [Test]
    public async Task SendFailure_WithoutLogger_WritesStderrDiagnostic()
    {
        var system = CreateSystem();
        var actor = await system.CreateAsync<SendFailureProbeActor>(new SendFailureProbe());

        var original = Console.Error;
        try
        {
            using var writer = new StringWriter();
            Console.SetError(writer);

            system.Send(actor.Id, new SendFailureBoom());

            // the loop reports asynchronously — poll with a bounded wait
            for (var i = 0; i < 100 && !writer.ToString().Contains("[PicoActor]"); i++)
                await Task.Delay(20);

            await Assert.That(writer.ToString()).Contains("[PicoActor]");
            await Assert.That(writer.ToString()).Contains(nameof(SendFailureBoom));
        }
        finally
        {
            Console.SetError(original);
        }
    }

    [Test]
    public async Task Send_WhenMailboxClosed_ThrowsInsteadOfSilentDrop()
    {
        var system = CreateSystem();
        var actor = await system.CreateAsync<SendFailureProbeActor>(new SendFailureProbe());

        // Mailbox closed while the registry entry is still present (stop-race window).
        actor.SignalStop();

        await Assert
            .That(() => system.Send(actor.Id, new SendFailureProbe()))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task SendFailure_WithBrokenStderr_DoesNotKillTheLoop()
    {
        var system = CreateSystem();
        var actor = await system.CreateAsync<SendFailureProbeActor>(new SendFailureProbe());

        var original = Console.Error;
        try
        {
            Console.SetError(new ThrowingTextWriter(original));

            // the diagnostic sink itself throws — the actor loop must survive it
            system.Send(actor.Id, new SendFailureBoom());

            // The loop must still be ALIVE after the broken sink: a merely completed
            // Ask is not proof — FailPendingEnvelopes would fault a queued Ask even if
            // the loop died. The probe must complete successfully.
            var probe = system.AskAsync<object?>(actor.Id, new SendFailureProbe()).AsTask();
            var completed = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(3)));
            await Assert.That(ReferenceEquals(completed, probe)).IsTrue();
            await Assert.That(probe.IsCompletedSuccessfully).IsTrue();
        }
        finally
        {
            Console.SetError(original);
        }
    }

    /// <summary>
    /// A stderr sink that fails the way a real broken sink does — but ONLY for the
    /// PicoActor diagnostic under test. Console.Error is process-global, and the test
    /// runner subscribes to <c>TaskScheduler.UnobservedTaskException</c> and reports it
    /// through Console.Error from the FINALIZER thread
    /// (TUnit.Engine.TUnitProcessInitializer.Initialise). A blanket throwing writer
    /// therefore turns any unrelated leaked faulted Task into an unhandled IOException on
    /// a finalizer thread: the process dies with 0xE0434352 mid-run and the console
    /// summary then lists only the tests that had already finished (observed 130/147 and
    /// 101/147 with failed: 0 / skipped: 0; the [NotInParallel] lane runs last, so it is
    /// always in the missing set). Forwarding every non-diagnostic write keeps the sink's
    /// failure semantics for the code under test and removes that process-global hazard.
    /// </summary>
    private sealed class ThrowingTextWriter : TextWriter
    {
        private readonly TextWriter _fallback;

        public ThrowingTextWriter(TextWriter fallback) => _fallback = fallback;

        public override Encoding Encoding => Encoding.UTF8;

        public override void WriteLine(string? value)
        {
            if (IsActorDiagnostic(value))
                throw new IOException("broken stderr");
            _fallback.WriteLine(value);
        }

        public override void Write(string? value)
        {
            if (IsActorDiagnostic(value))
                throw new IOException("broken stderr");
            _fallback.Write(value);
        }

        private static bool IsActorDiagnostic(string? value) =>
            value is not null && value.Contains("[PicoActor]", StringComparison.Ordinal);
    }

    [Test]
    public async Task BrokenStderrSink_ForwardsWritesThatAreNotActorDiagnostics()
    {
        var fallback = new StringWriter();
        var sink = new ThrowingTextWriter(fallback);

        // Not the code under test: the runner reports unobserved task exceptions through
        // Console.Error from the finalizer thread — that write must survive.
        sink.WriteLine("unrelated runner diagnostic");
        await Assert.That(fallback.ToString()).Contains("unrelated runner diagnostic");

        await Assert.That(() => sink.WriteLine("[PicoActor] boom")).Throws<IOException>();
        await Assert.That(() => sink.Write("[PicoActor] boom")).Throws<IOException>();
    }
}
