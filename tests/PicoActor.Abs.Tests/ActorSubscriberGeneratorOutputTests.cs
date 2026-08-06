using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;
using PicoDI.Abs;
using PicoMediator.Abs;

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

    private static GeneratorDriver RunGenerator(string assemblyName)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var inputTree = CSharpSyntaxTree.ParseText(InputSource, parseOptions);
        var compilation = CSharpCompilation.Create(
            assemblyName: assemblyName,
            syntaxTrees: [inputTree],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

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
        refs.Add(MetadataReference.CreateFromFile(typeof(IDomainEvent).Assembly.Location));
        refs.Add(MetadataReference.CreateFromFile(typeof(IEvent).Assembly.Location));
        refs.Add(MetadataReference.CreateFromFile(typeof(ISvcScope).Assembly.Location));
        return [.. refs];
    }

    private static string? FindGeneratedSource(GeneratorDriver driver, string fileNamePart)
    {
        var runResult = driver.GetRunResult();
        foreach (var result in runResult.Results)
        foreach (var source in result.GeneratedSources)
        if (source.HintName.Contains(fileNamePart, StringComparison.Ordinal))
            return source.SourceText.ToString();
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
        await Assert.That(CountOccurrences(source, "IDomainEventSubscriber<global::Paid>")).IsGreaterThanOrEqualTo(3);

        // exactly 2 bridge classes (Paid, Shipped) and 2 bridge registrations
        await Assert.That(CountOccurrences(source, "internal sealed class PicoActorEnvelopeBridge_")).IsEqualTo(2);
        await Assert.That(CountOccurrences(source, "static scope => new PicoActorEnvelopeBridge_")).IsEqualTo(2);
    }

    [Test]
    public async Task Bridge_NarrowsAndDeliversTypedEnvelope()
    {
        var driver = RunGenerator("App");
        var source = FindGeneratedSource(driver, "PicoActorSubscriberRegistrations")!;

        await Assert.That(source).Contains("if (envelope.Event is global::Paid e)");
        await Assert.That(source)
            .Contains("_scope.TryGetServices(typeof(global::PicoActor.Abs.IDomainEventSubscriber<global::Paid>), out var rawHandlers)");
        await Assert.That(source)
            .Contains("new global::PicoActor.Abs.DomainEventEnvelope<global::Paid>(envelope.ActorId, envelope.Version, e)");
        await Assert.That(source).Contains("exceptions ??= []");
        await Assert.That(source).Contains("throw new global::System.AggregateException(exceptions)");
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
}
