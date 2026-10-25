using Microsoft.CodeAnalysis;

namespace PicoActor.Abs.Tests;

// Declare-and-subscribe silently did nothing for shapes the generator cannot emit:
// generic handlers (no closed type to `new`) and file-local handlers (unreachable from
// another syntax tree) returned an empty result with no explanation. A subscriber that
// compiles but never fires is the worst failure mode — these shapes now report PICA002.
public sealed class FixCUnregisterableShapeDiagnosticsTests
{
    private const string GenericHandlerSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed class GenericHandler<T> : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private const string NestedInGenericOuterSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed class Outer<T>
        {
            public sealed class NestedHandler : IDomainEventSubscriber<Paid>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
        }
        """;

    private const string FileLocalHandlerSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        file sealed class FileLocalHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private const string PlainHandlerSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed class PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private const string PrivateCtorHandlerSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed class PrivateCtorHandler : IDomainEventSubscriber<Paid>
        {
            private PrivateCtorHandler() { }

            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private static int LineOf(string source, string marker)
    {
        var index = source.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0)
            throw new ArgumentException($"'{marker}' not found", nameof(marker));
        return source[..index].Count(static c => c == '\n');
    }

    private static Diagnostic[] Diagnostics(GeneratorDriver driver, string id) =>
        [.. driver.GetRunResult().Diagnostics.Where(d => d.Id == id)];

    [Test]
    public async Task GenericHandler_ReportsPICA002_AtTheDeclaration()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", GenericHandlerSource);
        var diagnostics = Diagnostics(driver, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].Severity).IsEqualTo(DiagnosticSeverity.Warning);
        await Assert.That(diagnostics[0].GetMessage()).Contains("GenericHandler");
        await Assert
            .That(diagnostics[0].Location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(GenericHandlerSource, "public sealed class GenericHandler"));
    }

    [Test]
    public async Task GenericHandler_IsNotEmittedIntoGeneratedCode()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", GenericHandlerSource);

        // nothing registrable is left → no configurator at all, instead of uncompilable code
        var source = FixCGeneratorHarness.FindGeneratedSource(
            driver,
            FixCGeneratorHarness.HintNamePart
        );
        await Assert.That(source is null || !source.Contains("GenericHandler")).IsTrue();
    }

    [Test]
    public async Task HandlerNestedInGenericOuter_ReportsPICA002()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", NestedInGenericOuterSource);
        var diagnostics = Diagnostics(driver, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].GetMessage()).Contains("NestedHandler");
    }

    [Test]
    public async Task FileLocalHandler_ReportsPICA002_AtTheDeclaration()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", FileLocalHandlerSource);
        var diagnostics = Diagnostics(driver, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].GetMessage()).Contains("FileLocalHandler");
        await Assert
            .That(diagnostics[0].Location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(FileLocalHandlerSource, "file sealed class FileLocalHandler"));
    }

    [Test]
    public async Task PlainHandler_ReportsNoPICA002()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", PlainHandlerSource);

        await Assert.That(Diagnostics(driver, "PICA002")).IsEmpty();
    }

    [Test]
    public async Task PICA001_CarriesTheDeclarationLocation()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", PrivateCtorHandlerSource);
        var diagnostics = Diagnostics(driver, "PICA001");

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert
            .That(diagnostics[0].Location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(PrivateCtorHandlerSource, "public sealed class PrivateCtorHandler"));
    }
}
