using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Shared harness for the Task C generator tests. Kept in its own file (not in
// ActorSubscriberGeneratorOutputTests) so the existing generator suite stays
// untouched: these tests cover partial-declaration dedupe, the PICA002 skip
// diagnostic, and the consumer language-version floor.
internal static class FixCGeneratorHarness
{
    public const string HintNamePart = "PicoActorSubscriberRegistrations";

    public static CSharpCompilation CreateCompilation(
        string assemblyName,
        string source,
        LanguageVersion languageVersion = LanguageVersion.Preview,
        string[]? extraSources = null
    )
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(languageVersion);
        var trees = new List<SyntaxTree> { CSharpSyntaxTree.ParseText(source, parseOptions) };
        foreach (var extra in extraSources ?? [])
            trees.Add(CSharpSyntaxTree.ParseText(extra, parseOptions));

        return CSharpCompilation.Create(
            assemblyName: assemblyName,
            syntaxTrees: trees,
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );
    }

    public static GeneratorDriver RunGenerator(
        string assemblyName,
        string source,
        LanguageVersion languageVersion = LanguageVersion.Preview
    )
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(languageVersion);
        var compilation = CreateCompilation(assemblyName, source, languageVersion);
        return CSharpGeneratorDriver
            .Create(
                [new ActorSubscriberGenerator().AsSourceGenerator()],
                parseOptions: parseOptions
            )
            .RunGeneratorsAndUpdateCompilation(compilation, out _, out _);
    }

    /// <summary>
    /// Runs the generator and compiles its output at <paramref name="languageVersion"/>,
    /// returning every error diagnostic (input code errors included).
    /// </summary>
    public static IReadOnlyList<Diagnostic> CompileWithGenerator(
        string assemblyName,
        string source,
        LanguageVersion languageVersion
    )
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(languageVersion);
        var compilation = CreateCompilation(assemblyName, source, languageVersion);
        var driver = CSharpGeneratorDriver
            .Create(
                [new ActorSubscriberGenerator().AsSourceGenerator()],
                parseOptions: parseOptions
            )
            .RunGeneratorsAndUpdateCompilation(compilation, out var output, out _);

        // Keep the driver alive: without a reference to it the generated trees of
        // `output` are still bound, so this is only about readability.
        _ = driver;

        return [.. output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error)];
    }

    public static MetadataReference[] GetMetadataReferences()
    {
        var trustedPlatformAssemblies = (
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
        )!.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        var refs = trustedPlatformAssemblies
            .Select(static p => MetadataReference.CreateFromFile(p))
            .ToList();

        // AppContext.BaseDirectory, not Assembly.Location: the latter is empty under
        // single-file/AOT packaging and trips IL3000 once the AOT analyzer is on.
        foreach (
            var name in (string[])["PicoActor.Abs.dll", "PicoMediator.Abs.dll", "PicoDI.Abs.dll"]
        )
            refs.Add(
                MetadataReference.CreateFromFile(Path.Combine(AppContext.BaseDirectory, name))
            );

        return [.. refs];
    }

    public static string? FindGeneratedSource(GeneratorDriver driver, string fileNamePart)
    {
        foreach (var result in driver.GetRunResult().Results)
        {
            foreach (var source in result.GeneratedSources)
            {
                if (source.HintName.Contains(fileNamePart, StringComparison.Ordinal))
                    return source.SourceText.ToString();
            }
        }

        return null;
    }

    public static int CountOccurrences(string source, string marker) =>
        source.Split(marker, StringSplitOptions.None).Length - 1;
}
