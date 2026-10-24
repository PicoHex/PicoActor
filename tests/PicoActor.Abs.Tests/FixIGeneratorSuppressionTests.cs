using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Task I: PICA001/PICA002 must stay SUPPRESSIBLE.
//
// The value-only pipeline (Task G) rebuilt the diagnostic position at output time with
// Location.Create(path, span, lineSpan), which yields a Location of kind ExternalFile:
// SourceTree == null, IsInSource == false. Roslyn resolves `#pragma warning disable` and a
// per-file `.editorconfig` severity through the diagnostic's SyntaxTree, so a location that
// is not bound to a tree silently disables both (#pragma and `dotnet_diagnostic.PICA001
// .severity = none`), while <NoWarn>/TreatWarningsAsErrors keep working because they do not
// consult the tree at all.
//
// WHAT THESE TESTS CANNOT DO: assert on a suppressed-with-`#pragma` diagnostic directly.
// In-process, no public Roslyn API applies the compiler's filter to generator diagnostics —
// GeneratorDriver.GetRunResult().Diagnostics is the RAW set, and Compilation.GetDiagnostics()
// does not contain generator diagnostics at all (measured on Microsoft.CodeAnalysis 5.9.0).
// Suppression therefore has to be verified against a real build (evidence in the task
// report: the same source compiled with `dotnet build` before/after the fix). What is
// asserted here is the precise property that makes suppression possible: the location must
// be a SOURCE location bound to the offending tree, at the declaration's own span.
public sealed class FixIGeneratorSuppressionTests
{
    private const string SharedUsings = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;
        """;

    // A subscriber the generator cannot emit (PICA002, Warning): a generic type has no
    // closed type to `new` in generated code.
    private const string GenericHandler = """
        public record Paid(int Id) : IDomainEvent;

        public sealed class GenericHandler<T> : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    // A subscriber the generated factory cannot construct (PICA001, Error): no accessible
    // instance constructor.
    private const string PrivateCtorHandler = """
        public record Paid(int Id) : IDomainEvent;

        public sealed class PrivateCtorHandler : IDomainEventSubscriber<Paid>
        {
            private PrivateCtorHandler() { }

            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    private static CSharpParseOptions ParseOptions =>
        CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);

    /// <summary>
    /// Runs the generator over one file and returns the run result plus the input tree, so a
    /// diagnostic's location can be checked against the tree it claims to point into.
    /// </summary>
    private static (GeneratorRunResult Result, SyntaxTree Tree) RunOn(string path, string source)
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

        return (driver.GetRunResult().Results[0], tree);
    }

    private static Diagnostic[] Reported(GeneratorRunResult result, string id) =>
        [.. result.Diagnostics.Where(d => d.Id == id)];

    private static int LineOf(string source, string marker)
    {
        var index = source.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0)
            throw new ArgumentException($"'{marker}' not found", nameof(marker));
        return source[..index].Count(static c => c == '\n');
    }

    // == 1. PICA002 carries a source-bound location (the precondition for #pragma) ======
    // Same file carries `#pragma warning disable PICA002`: for the pragma to take effect the
    // reported location must be IN SOURCE and point at the declaration, not at an external
    // "file" that Roslyn cannot map back to a directive scope.

    [Test]
    public async Task Pica002_LocationIsInSource_SoPragmaApplies()
    {
        const string marker = "public sealed class GenericHandler";
        var source =
            SharedUsings
            + """

                #pragma warning disable PICA002

                """
            + GenericHandler;

        var (result, tree) = RunOn("Input.cs", source);
        var diagnostics = Reported(result, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);

        var location = diagnostics[0].Location;
        await Assert.That(location.IsInSource).IsTrue();
        await Assert.That(location.Kind).IsEqualTo(LocationKind.SourceFile);
        await Assert.That(ReferenceEquals(location.SourceTree, tree)).IsTrue();
        await Assert.That(location.GetLineSpan().Path).IsEqualTo("Input.cs");
        await Assert
            .That(location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
        await Assert.That(location.SourceSpan.Start).IsGreaterThan(0);
    }

    // == 2. PICA001, same precondition =================================================

    [Test]
    public async Task Pica001_LocationIsInSource_SoPragmaApplies()
    {
        const string marker = "public sealed class PrivateCtorHandler";
        var source =
            SharedUsings
            + """

                #pragma warning disable PICA001

                """
            + PrivateCtorHandler;

        var (result, tree) = RunOn("Input.cs", source);
        var diagnostics = Reported(result, "PICA001");

        await Assert.That(diagnostics.Length).IsEqualTo(1);

        var location = diagnostics[0].Location;
        await Assert.That(location.IsInSource).IsTrue();
        await Assert.That(location.Kind).IsEqualTo(LocationKind.SourceFile);
        await Assert.That(ReferenceEquals(location.SourceTree, tree)).IsTrue();
        await Assert
            .That(location.GetLineSpan().StartLinePosition.Line)
            .IsEqualTo(LineOf(source, marker));
    }

    // == 3. guard: the diagnostic is still reported, and still locates the declaration ===

    [Test]
    public async Task Pica002_IsStillReportedWithoutPragma()
    {
        const string marker = "public sealed class GenericHandler";
        var source = SharedUsings + "\n" + GenericHandler;
        var (result, _) = RunOn("Input.cs", source);
        var diagnostics = Reported(result, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].Severity).IsEqualTo(DiagnosticSeverity.Warning);
        await Assert.That(diagnostics[0].GetMessage()).Contains("GenericHandler");

        var lineSpan = diagnostics[0].Location.GetLineSpan();
        await Assert.That(lineSpan.Path).IsEqualTo("Input.cs");
        await Assert.That(lineSpan.StartLinePosition.Line).IsEqualTo(LineOf(source, marker));
        await Assert.That(diagnostics[0].Location.SourceSpan.Start).IsGreaterThan(0);
    }

    // == 4/5. which pipeline states may keep a Location at all =========================
    // The fix keeps a REAL Location for the rare diagnosable shapes only; everything else —
    // every healthy subscriber, i.e. the hot path — must keep travelling through the
    // incremental pipeline as values (Task G), or its whole point is lost. The state type is
    // private to the generator, and Roslyn's tracked steps do not expose its instances, so
    // the factories are exercised directly through reflection: the invariant worth pinning
    // is "a constructible subscriber can never retain a location", enforced by the factory
    // and not merely observed.

    private const string StateTypeName = "PicoActor.Gen.ActorSubscriberGenerator+SubscriberInfo";

    [Test]
    public async Task NormalSubscriber_DropsTheDiagnosticLocation()
    {
        var state = RegistrationState(hasAccessibleConstructor: true);

        await Assert.That(DiagnosticLocationOf(state)).IsNull();
    }

    [Test]
    public async Task UnconstructibleSubscriber_KeepsTheDiagnosticLocation()
    {
        var state = RegistrationState(hasAccessibleConstructor: false);

        var location = DiagnosticLocationOf(state);
        await Assert.That(location).IsNotNull();
        await Assert.That(location!.IsInSource).IsTrue();
    }

    private static object RegistrationState(bool hasAccessibleConstructor)
    {
        var location = ALocation();
        return InvokeFactory(
            "Registration",
            new Dictionary<string, object?>
            {
                ["constructorParameterTypes"] = ImmutableArray<string>.Empty,
                ["diagnosticLineSpan"] = location.GetLineSpan().Span,
                ["diagnosticLocation"] = location,
                ["diagnosticFilePath"] = "Input.cs",
                ["diagnosticSpan"] = location.SourceSpan,
                ["eventTypeFqn"] = "global::Paid",
                ["hasAccessibleConstructor"] = hasAccessibleConstructor,
                ["implementationType"] = "global::PaidHandler",
                ["registrationKey"] = "App::global::PaidHandler::global::Paid",
            }
        );
    }

    /// <summary>A real, source-bound Location: what the transform stage now hands to output.</summary>
    private static Location ALocation() =>
        Location.Create(
            CSharpSyntaxTree.ParseText("class C { }", ParseOptions, path: "Input.cs"),
            new TextSpan(6, 1)
        );

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Test-only reflection over the generator's private state type; the test binary is never trimmed."
    )]
    private static Type StateType =>
        typeof(ActorSubscriberGenerator).GetNestedType("SubscriberInfo", BindingFlags.NonPublic)
        ?? throw new InvalidOperationException(
            $"{StateTypeName} is gone — the pipeline state shape Task I pins no longer exists"
        );

    /// <summary>
    /// Calls a static factory on the pipeline state type, binding arguments BY PARAMETER
    /// NAME so a reordered signature does not silently swap values. Every parameter must be
    /// supplied and every supplied name must exist: adding or renaming a parameter fails
    /// loudly here instead of quietly weakening the test.
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

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Test-only reflection over the generator's private state type; the test binary is never trimmed."
    )]
    private static Location? DiagnosticLocationOf(object state)
    {
        var type = state.GetType();
        var property = type.GetProperty(
            "DiagnosticLocation",
            BindingFlags.Instance | BindingFlags.Public
        );
        if (property is not null)
            return (Location?)property.GetValue(state);

        var field = type.GetField(
            "DiagnosticLocation",
            BindingFlags.Instance | BindingFlags.Public
        );
        return field is null
            ? throw new InvalidOperationException(
                $"{type.FullName} has no public member 'DiagnosticLocation' — the pipeline "
                    + "state shape this test pins is gone"
            )
            : (Location?)field.GetValue(state);
    }
}
