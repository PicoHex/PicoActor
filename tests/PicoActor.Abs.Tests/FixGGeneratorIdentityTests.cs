using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Task G: the incremental pipeline must not keep Roslyn objects (INamedTypeSymbol /
// ITypeSymbol / Location) in its state — a symbol roots the Compilation and every
// SyntaxTree of the project it came from, which defeats the incremental cache.
//
// The replacement identity is a string key plus value-type positions, so these tests pin
// the OBSERVABLE behavior that must survive the change:
//   - partial declarations still collapse to one registration (string key, not symbol),
//   - two types that share a short name stay distinct (the FQN is in the key),
//   - PICA001/PICA002 still carry a real path + line + span (rebuilt, not stored),
//   - two independent Compilation instances over the same source generate identical bytes.
public sealed class FixGGeneratorIdentityTests
{
    private const string SharedUsings = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;
        """;

    private static CSharpParseOptions ParseOptions =>
        CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);

    /// <summary>
    /// Runs the generator over one or more (path, source) files. Paths matter here: the
    /// diagnostic test asserts on the file path a rebuilt Location reports.
    /// </summary>
    private static GeneratorDriver RunOn(
        string assemblyName,
        params (string Path, string Source)[] files
    )
    {
        var compilation = CreateCompilation(assemblyName, files);
        return CSharpGeneratorDriver
            .Create(
                [new ActorSubscriberGenerator().AsSourceGenerator()],
                parseOptions: ParseOptions
            )
            .RunGeneratorsAndUpdateCompilation(compilation, out _, out _);
    }

    private static CSharpCompilation CreateCompilation(
        string assemblyName,
        params (string Path, string Source)[] files
    )
    {
        var trees = new List<SyntaxTree>(files.Length);
        foreach (var (path, source) in files)
            trees.Add(CSharpSyntaxTree.ParseText(source, ParseOptions, path: path));

        return CSharpCompilation.Create(
            assemblyName: assemblyName,
            syntaxTrees: trees,
            references: FixCGeneratorHarness.GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );
    }

    private static string? GeneratedSource(GeneratorDriver driver) =>
        FixCGeneratorHarness.FindGeneratedSource(driver, FixCGeneratorHarness.HintNamePart);

    private static Diagnostic[] Diagnostics(GeneratorDriver driver, string id) =>
        [.. driver.GetRunResult().Diagnostics.Where(d => d.Id == id)];

    private static int IndexOf(string source, string marker)
    {
        var index = source.IndexOf(marker, StringComparison.Ordinal);
        if (index < 0)
            throw new ArgumentException($"'{marker}' not found", nameof(marker));
        return index;
    }

    private static int LineOf(string source, string marker) =>
        source[..IndexOf(source, marker)].Count(static c => c == '\n');

    private static int ColumnOf(string source, string marker)
    {
        var index = IndexOf(source, marker);
        return index - (source.LastIndexOf('\n', index) + 1);
    }

    // == 1. partial declarations, base list in BOTH parts, spread over TWO files ======
    // The scan is syntax-driven: each declaration is visited while GetDeclaredSymbol keeps
    // returning the merged type. Both visits must produce the same STRING key.

    private static readonly string PartialPartOne =
        SharedUsings
        + """

            public record Paid(int Id) : IDomainEvent;

            public sealed partial class PaidHandler : IDomainEventSubscriber<Paid>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
            """;

    private static readonly string PartialPartTwo =
        SharedUsings
        + """

            public sealed partial class PaidHandler : IDomainEventSubscriber<Paid>
            {
            }
            """;

    [Test]
    public async Task PartialTypeAcrossTwoFiles_RegistersHandlerExactlyOnce()
    {
        var driver = RunOn("App", ("Part1.cs", PartialPartOne), ("Part2.cs", PartialPartTwo));
        var source = GeneratedSource(driver);

        await Assert.That(source).IsNotNull();
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "static _ => new global::PaidHandler()"
                )
            )
            .IsEqualTo(1);
        await Assert.That(Diagnostics(driver, "PICA001")).IsEmpty();
        await Assert.That(Diagnostics(driver, "PICA002")).IsEmpty();
    }

    // == 2. same short name, different namespaces ====================================
    // A string identity is only safe when it is the FULLY QUALIFIED name: a short-name
    // key would silently merge these two handlers into one registration.

    private static readonly string SameShortNameSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;

        public record Paid(int Id) : IDomainEvent;

        namespace Alpha
        {
            public sealed class Handler : IDomainEventSubscriber<Paid>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
        }

        namespace Beta
        {
            public sealed class Handler : IDomainEventSubscriber<Paid>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
        }
        """;

    [Test]
    public async Task HandlersSharingAShortNameInDifferentNamespaces_AreBothRegisteredOnce()
    {
        var driver = RunOn("App", ("Input.cs", SameShortNameSource));
        var source = GeneratedSource(driver);

        await Assert.That(source).IsNotNull();
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "static _ => new global::Alpha.Handler()"
                )
            )
            .IsEqualTo(1);
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "static _ => new global::Beta.Handler()"
                )
            )
            .IsEqualTo(1);

        // one bridge: both handlers subscribe to the same event type
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "internal sealed class PicoActorEnvelopeBridge_"
                )
            )
            .IsEqualTo(1);
    }

    // == 3. PICA002 keeps a REAL position after the Location object is gone ==========
    // The diagnostic position is rebuilt from (path, TextSpan, LinePositionSpan) at output
    // time, so the reported location must still name the file, the line, and a span that
    // actually points at the declaration text.

    private static readonly string FileLocalHandlerSource =
        SharedUsings
        + """

            public record Paid(int Id) : IDomainEvent;

            file sealed class FileLocalHandler : IDomainEventSubscriber<Paid>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
            """;

    [Test]
    public async Task PICA002_CarriesRealPathLineAndSpan()
    {
        const string path = "Input.cs";
        var driver = RunOn("App", (path, FileLocalHandlerSource));
        var diagnostics = Diagnostics(driver, "PICA002");

        await Assert.That(diagnostics.Length).IsEqualTo(1);

        var location = diagnostics[0].Location;
        await Assert.That(location).IsNotEqualTo(Location.None);

        var lineSpan = location.GetLineSpan();
        await Assert.That(lineSpan.Path).IsEqualTo(path);
        await Assert
            .That(lineSpan.StartLinePosition.Line)
            .IsEqualTo(LineOf(FileLocalHandlerSource, "file sealed class FileLocalHandler"));

        // A declaration location is its IDENTIFIER token, so that exact span and column
        // must survive the trip through the value representation (path/span/lineSpan)
        // instead of collapsing to a default(TextSpan) at offset 0.
        await Assert.That(location.SourceSpan.Start).IsGreaterThan(0);
        await Assert.That(location.SourceSpan.Length).IsEqualTo("FileLocalHandler".Length);
        await Assert
            .That(FileLocalHandlerSource[location.SourceSpan.Start..])
            .StartsWith("FileLocalHandler : IDomainEventSubscriber<Paid>");
        await Assert
            .That(lineSpan.StartLinePosition.Character)
            .IsEqualTo(ColumnOf(FileLocalHandlerSource, "FileLocalHandler :"));
    }

    // == 4. two independent Compilation instances -> identical output =================
    // No pipeline state may depend on an object identity that differs between runs:
    // re-parsing the same source must produce the very same bytes, and the incremental
    // node must be able to serve the output from cache.

    [Test]
    public async Task TwoIndependentCompilationsOfTheSameSource_ProduceIdenticalOutput()
    {
        var driver = CSharpGeneratorDriver.Create(
            [new ActorSubscriberGenerator().AsSourceGenerator()],
            parseOptions: ParseOptions,
            driverOptions: new GeneratorDriverOptions(
                IncrementalGeneratorOutputKind.None,
                trackIncrementalGeneratorSteps: true
            )
        );

        driver = (CSharpGeneratorDriver)
            driver.RunGenerators(
                CreateCompilation("App", ("Input.cs", PartialPartOne), ("Other.cs", PartialPartTwo))
            );
        var firstRun = Snapshot(driver);

        driver = (CSharpGeneratorDriver)
            driver.RunGenerators(
                CreateCompilation("App", ("Input.cs", PartialPartOne), ("Other.cs", PartialPartTwo))
            );
        var secondRun = Snapshot(driver);

        await Assert.That(firstRun.Length).IsGreaterThan(0);
        await Assert.That(secondRun.Length).IsEqualTo(firstRun.Length);
        for (var i = 0; i < firstRun.Length; i++)
        {
            await Assert.That(secondRun[i].HintName).IsEqualTo(firstRun[i].HintName);
            await Assert.That(secondRun[i].Text).IsEqualTo(firstRun[i].Text);
        }

        var reasons = driver
            .GetRunResult()
            .Results[0]
            .TrackedOutputSteps.SelectMany(static kv => kv.Value)
            .SelectMany(static step => step.Outputs)
            .Select(static o => o.Reason)
            .ToArray();

        await Assert.That(reasons).IsNotEmpty();
        await Assert
            .That(
                reasons.All(static r =>
                    r == IncrementalStepRunReason.Cached || r == IncrementalStepRunReason.Unchanged
                )
            )
            .IsTrue();
    }

    private static (string HintName, string Text)[] Snapshot(GeneratorDriver driver) =>
        [
            .. driver
                .GetRunResult()
                .Results.SelectMany(static r => r.GeneratedSources)
                .Select(static s => (s.HintName, s.SourceText.ToString())),
        ];
}
