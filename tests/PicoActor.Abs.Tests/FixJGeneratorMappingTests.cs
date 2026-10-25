using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Task J1: a reported diagnostic must keep the source's `#line` MAPPING.
//
// The generator reduces a declaration position to plain values (Task G) and rebuilds a
// Location at output time — from the UNMAPPED line span only, which drops
// `#line 200 "Mapped.cs"`: a consumer that runs a code generator ahead of this one saw
// `Subscriber.cs(7,19)` where the compiler would report `Mapped.cs(200,19)`.
//
// A Location carries two spans: the physical one (GetLineSpan) and the mapped one
// (GetMappedLineSpan — what the compiler prints and what an IDE navigates to). Both must
// survive the trip through the pipeline, whichever path materializes the location.
public sealed class FixJGeneratorMappingTests
{
    private const string SharedUsings = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;
        """;

    private const string GenericSubscriber = """
        public sealed class GenericHandler<T> : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private const string PaidEvent = """
        public record Paid(int Id) : IDomainEvent;

        """;

    private static readonly CSharpParseOptions ParseOptions =
        CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);

    private static GeneratorRunResult RunOn(string path, string source)
    {
        var tree = CSharpSyntaxTree.ParseText(source, ParseOptions, path: path);
        var compilation = CSharpCompilation.Create(
            assemblyName: "App",
            syntaxTrees: [tree],
            references: FixCGeneratorHarness.GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        var driver = CSharpGeneratorDriver
            .Create(
                [new ActorSubscriberGenerator().AsSourceGenerator()],
                parseOptions: ParseOptions
            )
            .RunGeneratorsAndUpdateCompilation(compilation, out _, out _);

        return driver.GetRunResult().Results[0];
    }

    private static Diagnostic SinglePica002(GeneratorRunResult result) =>
        result.Diagnostics.Single(d => d.Id == "PICA002");

    private static int LineOf(string source, string marker)
    {
        var index = source.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0)
            throw new ArgumentException($"'{marker}' not found", nameof(marker));
        return source[..index].Count(static c => c == '\n');
    }

    // == 1. `#line` mapping survives ===================================================
    // The declaration sits on the first line after the directive, so it must report as
    // Mapped.cs line 200 (1-based) — and its PHYSICAL position must stay Subscriber.cs.

    [Test]
    public async Task Pica002_KeepsLineDirectiveMapping()
    {
        const string marker = "public sealed class GenericHandler";
        var source =
            SharedUsings
            + "\n"
            + PaidEvent
            + "#line 200 \"Mapped.cs\"\n"
            + GenericSubscriber
            + "\n#line default\n";

        var location = SinglePica002(RunOn("Subscriber.cs", source)).Location;

        var mapped = location.GetMappedLineSpan();
        await Assert.That(mapped.Path).IsEqualTo("Mapped.cs");
        await Assert.That(mapped.StartLinePosition.Line + 1).IsEqualTo(200);

        // The physical span is untouched: this is still the file that was compiled.
        await Assert.That(location.GetLineSpan().Path).IsEqualTo("Subscriber.cs");
        await Assert
            .That(location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
    }

    // == 1b. the REBUILT location keeps the mapping too =================================
    // A diagnosable shape keeps its real Location (Task I), and that is what makes the
    // compiler print Mapped.cs above. The pipeline state ALSO carries plain position values,
    // and the output stage rebuilds a Location from them when no real one was retained
    // (non-source declaration). Those values must carry the mapped span as well: rebuilding
    // from the physical span alone reports the unmapped file/line — exactly the regression
    // this test pins, on the one path the end-to-end test above cannot reach.
    //
    // The state type is private to the generator, so the factory and the rebuild are
    // exercised through reflection (same technique as FixIGeneratorSuppressionTests).

    [Test]
    public async Task RebuiltLocation_KeepsLineDirectiveMapping()
    {
        const string marker = "public sealed class GenericHandler";
        var source =
            SharedUsings
            + "\n"
            + PaidEvent
            + "#line 200 \"Mapped.cs\"\n"
            + GenericSubscriber
            + "\n#line default\n";

        var tree = CSharpSyntaxTree.ParseText(source, ParseOptions, path: "Subscriber.cs");
        var span = SpanOf(source, marker);

        // A diagnosable shape whose real Location was NOT retained: the rebuild path.
        var state = InvokeFactory(
            "Skipped",
            new Dictionary<string, object?>
            {
                ["diagnosticFilePath"] = "Subscriber.cs",
                ["diagnosticLocation"] = null,
                ["diagnosticLineSpan"] = tree.GetLineSpan(span).Span,
                ["diagnosticMappedFilePath"] = "Mapped.cs",
                ["diagnosticMappedLineSpan"] = tree.GetMappedLineSpan(span).Span,
                ["diagnosticSpan"] = span,
                ["implementationType"] = "global::GenericHandler<T>",
                ["reason"] =
                    "generic subscriber types cannot be instantiated by the generated factory",
                ["registrationKey"] = "App::global::GenericHandler<T>::",
            }
        );

        var location = RebuiltLocationOf(state);

        await Assert.That(location.GetMappedLineSpan().Path).IsEqualTo("Mapped.cs");
        await Assert.That(location.GetMappedLineSpan().StartLinePosition.Line + 1).IsEqualTo(200);
        await Assert.That(location.GetLineSpan().Path).IsEqualTo("Subscriber.cs");
        await Assert
            .That(location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
    }

    private static TextSpan SpanOf(string source, string marker)
    {
        var index = source.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0)
            throw new ArgumentException($"'{marker}' not found", nameof(marker));
        return new TextSpan(index, marker.Length);
    }

    private const string StateTypeName = "PicoActor.Gen.ActorSubscriberGenerator+SubscriberInfo";

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Test-only reflection over the generator's private state type; the test binary is never trimmed."
    )]
    private static Type StateType =>
        typeof(ActorSubscriberGenerator).GetNestedType("SubscriberInfo", BindingFlags.NonPublic)
        ?? throw new InvalidOperationException($"{StateTypeName} is gone");

    /// <summary>
    /// Calls a static factory on the pipeline state type, binding arguments BY PARAMETER
    /// NAME: every parameter must be supplied and every supplied name must exist, so adding
    /// or renaming a parameter fails loudly here instead of silently weakening the test.
    /// </summary>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Test-only reflection over the generator's private state type; the test binary is never trimmed."
    )]
    private static object InvokeFactory(string name, Dictionary<string, object?> arguments)
    {
        var method =
            StateType.GetMethod(name, BindingFlags.Static | BindingFlags.Public)
            ?? throw new InvalidOperationException($"{StateTypeName}.{name} is gone");

        var parameters = method.GetParameters();
        var bound = new object?[parameters.Length];
        var used = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            if (!arguments.TryGetValue(parameter.Name!, out var value))
                throw new InvalidOperationException(
                    $"{name} has a parameter '{parameter.Name}' this test does not supply"
                );
            bound[i] = value;
            used.Add(parameter.Name!);
        }

        var unused = arguments.Keys.Where(k => !used.Contains(k)).ToArray();
        if (unused.Length > 0)
            throw new InvalidOperationException(
                $"{name} has no parameter(s) {string.Join(", ", unused)}"
            );

        return method.Invoke(obj: null, bound)!;
    }

    /// <summary>
    /// The output stage's location materialization for a state that did NOT keep a real
    /// source Location — the only path where plain position values are used.
    /// </summary>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Test-only reflection over the generator's private rebuild helper."
    )]
    private static Location RebuiltLocationOf(object state)
    {
        var method =
            typeof(ActorSubscriberGenerator).GetMethod(
                "DiagnosticLocation",
                BindingFlags.Static | BindingFlags.NonPublic
            )
            ?? throw new InvalidOperationException(
                "ActorSubscriberGenerator.DiagnosticLocation is gone"
            );

        return (Location)method.Invoke(obj: null, [state])!;
    }

    // == 2. guard: without `#line` nothing changes ======================================

    [Test]
    public async Task Pica002_WithoutLineDirective_KeepsRealFileAndLine()
    {
        const string marker = "public sealed class GenericHandler";
        var source = SharedUsings + "\n" + PaidEvent + GenericSubscriber;

        var location = SinglePica002(RunOn("Subscriber.cs", source)).Location;

        await Assert.That(location.GetMappedLineSpan().Path).IsEqualTo("Subscriber.cs");
        await Assert
            .That(location.GetMappedLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
        await Assert.That(location.GetLineSpan().Path).IsEqualTo("Subscriber.cs");
        await Assert
            .That(location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
    }
}
