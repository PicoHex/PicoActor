using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Verifies the REAL generator output — not mirrored strings. Guards the
// multi-subscriber invariant: one envelope bridge per DISTINCT event type
// (not per handler class), so two handlers for the same event both receive it.
public sealed class ActorSubscriberGeneratorOutputTests
{
    private const string InputSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed class PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public sealed class PaidAuditHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public record Shipped(Guid Id) : IDomainEvent;

        public sealed class ShippedHandler : IDomainEventSubscriber<Shipped>
        {
            public ValueTask Handle(DomainEventEnvelope<Shipped> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        // A PicoMediator subscriber must NOT be picked up — namespace-locked scan.
        public sealed class PaidMediatorSub : ISubscriber<Paid>
        {
            public ValueTask Handle(Paid e, CancellationToken ct) => ValueTask.CompletedTask;
        }
        """;

    private static CSharpCompilation CreateCompilation(string assemblyName, string source)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var inputTree = CSharpSyntaxTree.ParseText(source, parseOptions);
        return CSharpCompilation.Create(
            assemblyName: assemblyName,
            syntaxTrees: [inputTree],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );
    }

    private static GeneratorDriver RunGenerator(string assemblyName, string? source = null)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation = CreateCompilation(assemblyName, source ?? InputSource);

        var generator = new ActorSubscriberGenerator();
        return CSharpGeneratorDriver
            .Create([generator.AsSourceGenerator()], parseOptions: parseOptions)
            .RunGeneratorsAndUpdateCompilation(compilation, out _, out _);
    }

    private static MetadataReference[] GetMetadataReferences()
    {
        var trustedPlatformAssemblies = (
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
        )!.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        var refs = trustedPlatformAssemblies
            .Select(static p => MetadataReference.CreateFromFile(p))
            .ToList();

        // AppContext.BaseDirectory, not Assembly.Location: the latter is empty under
        // single-file/AOT packaging and trips IL3000 once the AOT analyzer is on.
        refs.Add(
            MetadataReference.CreateFromFile(
                Path.Combine(AppContext.BaseDirectory, "PicoActor.Abs.dll")
            )
        );
        refs.Add(
            MetadataReference.CreateFromFile(
                Path.Combine(AppContext.BaseDirectory, "PicoMediator.Abs.dll")
            )
        );
        refs.Add(
            MetadataReference.CreateFromFile(
                Path.Combine(AppContext.BaseDirectory, "PicoDI.Abs.dll")
            )
        );
        return [.. refs];
    }

    private static string? FindGeneratedSource(GeneratorDriver driver, string fileNamePart)
    {
        var runResult = driver.GetRunResult();
        foreach (var result in runResult.Results)
        {
            foreach (var source in result.GeneratedSources)
            {
                if (source.HintName.Contains(fileNamePart, StringComparison.Ordinal))
                    return source.SourceText.ToString();
            }
        }

        return null;
    }

    private static int CountOccurrences(string source, string marker) =>
        source.Split(marker, StringSplitOptions.None).Length - 1;

    [Test]
    public async Task ConfiguratorId_IsStablePerAssembly()
    {
        foreach (var asm in new[] { "myapp", "PicoActor.Tests", "Zapp" })
        {
            var driver = RunGenerator(asm);
            var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations");
            await Assert.That(source).IsNotNull();
            await Assert.That(source!).Contains($"pico-actor::{asm}");
        }
    }

    [Test]
    public async Task OneBridgePerEventType_NotPerHandlerClass()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        // 3 handler classes (Paid ×2, Shipped ×1) → 3 handler registrations
        await Assert
            .That(CountOccurrences(source, "IDomainEventSubscriber<global::Paid>"))
            .IsGreaterThanOrEqualTo(3);

        // exactly 2 bridge classes (Paid, Shipped) and 2 bridge registrations
        await Assert
            .That(CountOccurrences(source, "internal sealed class PicoActorEnvelopeBridge_"))
            .IsEqualTo(2);
        await Assert
            .That(CountOccurrences(source, "static scope => new PicoActorEnvelopeBridge_"))
            .IsEqualTo(2);
    }

    [Test]
    public async Task Bridge_NarrowsAndDeliversTypedEnvelope()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        await Assert.That(source).Contains("if (envelope.Event is global::Paid e)");
        await Assert
            .That(source)
            .Contains(
                "_scope.TryGetServices(typeof(global::PicoActor.Abs.IDomainEventSubscriber<global::Paid>), out var rawHandlers)"
            );
        await Assert
            .That(source)
            .Contains(
                "new global::PicoActor.Abs.DomainEventEnvelope<global::Paid>(envelope.ActorId, envelope.Version, e)"
            );
        await Assert.That(source).Contains("exceptions ??= []");
        await Assert
            .That(source)
            .Contains("throw new global::System.AggregateException(exceptions)");
    }

    [Test]
    public async Task Subscriber_InNamespaceLockedToPicoActorAbs()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        // the generator must only react to PicoActor.Abs.IDomainEventSubscriber`1 —
        // a PicoMediator ISubscriber in the input must NOT generate anything extra
        await Assert.That(source).DoesNotContain("ISubscriber<global::Paid>");
    }

    // ═══════════════════════════════════════════════════════════
    // Incremental behavior
    // ═══════════════════════════════════════════════════════════

    /// <summary>
    /// An edit that changes neither the subscriber set nor the assembly name must not
    /// re-run source generation: a Compilation instance changes on every edit, so feeding
    /// the whole CompilationProvider into RegisterSourceOutput invalidates the output on
    /// every keystroke.
    /// </summary>
    [Test]
    public async Task Generator_UnrelatedEdit_DoesNotRerunSourceOutput()
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation1 = CreateCompilation("App", InputSource);
        var compilation2 = compilation1.AddSyntaxTrees(
            CSharpSyntaxTree.ParseText(
                "internal sealed class UnrelatedEdit { } // no subscriber here",
                parseOptions
            )
        );

        var driver = CSharpGeneratorDriver.Create(
            [new ActorSubscriberGenerator().AsSourceGenerator()],
            parseOptions: parseOptions,
            driverOptions: new GeneratorDriverOptions(
                IncrementalGeneratorOutputKind.None,
                trackIncrementalGeneratorSteps: true
            )
        );

        driver = (CSharpGeneratorDriver)driver.RunGenerators(compilation1);
        driver = (CSharpGeneratorDriver)driver.RunGenerators(compilation2);

        var reasons = driver
            .GetRunResult()
            .Results[0]
            .TrackedOutputSteps.SelectMany(kv => kv.Value)
            .SelectMany(step => step.Outputs)
            .Select(o => o.Reason)
            .ToArray();

        await Assert.That(reasons).IsNotEmpty();
        await Assert
            .That(
                reasons.All(r =>
                    r == IncrementalStepRunReason.Cached || r == IncrementalStepRunReason.Unchanged
                )
            )
            .IsTrue();
    }

    // ═══════════════════════════════════════════════════════════
    // Constructor selection / generated-shape robustness
    // ═══════════════════════════════════════════════════════════

    private const string MultiCtorSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public interface IMyDep { }

        public record Paid(int Id) : IDomainEvent;

        // Parameterless ctor declared FIRST: DI resolves the greediest ctor, so the
        // generated factory must pick the IMyDep one.
        public sealed class TwoCtorHandler : IDomainEventSubscriber<Paid>
        {
            private readonly IMyDep? _dep;

            public TwoCtorHandler() { }

            public TwoCtorHandler(IMyDep dep) => _dep = dep;

            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    [Test]
    public async Task HandlerWithMultipleConstructors_UsesGreediestConstructor()
    {
        var driver = RunGenerator("App", MultiCtorSource);
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        await Assert
            .That(source)
            .Contains(
                "new global::TwoCtorHandler((global::IMyDep)scope.GetService(typeof(global::IMyDep)))"
            );
    }

    private const string CollidingNamesSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        namespace My
        {
            public record Event(int Id) : IDomainEvent;
        }

        // Sanitizes to the same bridge identifier as global::My.Event
        public record My_Event(int Id) : IDomainEvent;

        public sealed class EventHandler : IDomainEventSubscriber<My.Event>
        {
            public ValueTask Handle(DomainEventEnvelope<My.Event> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public sealed class MyEventHandler : IDomainEventSubscriber<My_Event>
        {
            public ValueTask Handle(DomainEventEnvelope<My_Event> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    [Test]
    public async Task EventTypesWithCollidingSanitizedNames_ProduceDistinctBridgeClasses()
    {
        var driver = RunGenerator("App", CollidingNamesSource);
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        var bridgeNames = Regex
            .Matches(source, @"internal sealed class (PicoActorEnvelopeBridge_\w+) :")
            .Select(m => m.Groups[1].Value)
            .ToArray();

        // Two distinct event types → two distinct bridge classes (one file, so a name
        // collision would be a CS0101 duplicate-class error)
        await Assert.That(bridgeNames.Length).IsEqualTo(2);
        await Assert.That(bridgeNames.Distinct(StringComparer.Ordinal).Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Bridge_MissingCommandSenderRegistration_FailsWithDiagnostic()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        // Without AddPicoActor() the port is unregistered: the bridge must fail loudly
        // with an actionable message instead of passing null into the handler.
        await Assert.That(source).Contains("ICommandSender is not registered");
    }

    // ═══════════════════════════════════════════════════════════
    // Declaration-shape coverage: records are first-class handlers, and
    // unregistrable shapes must produce a diagnostic instead of broken code
    // ═══════════════════════════════════════════════════════════

    private const string RecordSubscriberSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed record PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    [Test]
    public async Task RecordSubscriber_IsDiscovered()
    {
        var driver = RunGenerator("App", RecordSubscriberSource);
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations");

        await Assert.That(source).IsNotNull();
        await Assert.That(source!).Contains("IDomainEventSubscriber<global::Paid>");
        await Assert.That(source!).Contains("new global::PaidHandler()");
    }

    private const string PrivateCtorSubscriberSource = """
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

    [Test]
    public async Task PrivateCtorSubscriber_ReportsDiagnostic_AndIsNotRegistered()
    {
        var driver = RunGenerator("App", PrivateCtorSubscriberSource);
        var result = driver.GetRunResult();

        // PICA001 instead of emitting `new PrivateCtorHandler()` (CS0122 in consumer builds)
        await Assert.That(result.Diagnostics.Any(d => d.Id == "PICA001")).IsTrue();

        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations");
        await Assert.That(source is null || !source.Contains("PrivateCtorHandler")).IsTrue();
    }

    private const string InternalCtorSubscriberSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public interface IMyDep { }

        public record Paid(int Id) : IDomainEvent;

        public sealed class InternalDepCtorHandler : IDomainEventSubscriber<Paid>
        {
            internal InternalDepCtorHandler(IMyDep dep) { }

            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    [Test]
    public async Task InternalCtorSubscriberWithDependency_IsResolvedViaDi()
    {
        var driver = RunGenerator("App", InternalCtorSubscriberSource);
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations");

        await Assert.That(source).IsNotNull();
        await Assert
            .That(source!)
            .Contains(
                "new global::InternalDepCtorHandler((global::IMyDep)scope.GetService(typeof(global::IMyDep)))"
            );
    }
}
