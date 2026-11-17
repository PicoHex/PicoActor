using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using PicoActor.EventStore.Gen;

namespace PicoActor.EventStore.Gen.Tests;

/// <summary>
/// Drives the REAL analyzer over an in-process compilation. The predicate it has to implement is
/// "this IDomainEvent is listed by a [PicoDerivedType] on one of its bases"; the two shapes around it
/// are the compile-clean, first-append-fatal ones that no other test in this repo can catch, because
/// they only fail on the first write.
/// </summary>
public sealed class AnalyzerTests
{
    private const string DiagnosticId = "PICOSTORE001";

    [Test, Timeout(30000)]
    public async Task Analyzer_FlagsUnregisteredEvent(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        // A concrete IDomainEvent with no registration anywhere: PicoJetson generates no serializer
        // for it, so this only explodes on the first append.
        const string source = """
            using PicoActor.Abs;
            using PicoSerDe.Core;

            public sealed record OrphanEvent(int Delta) : IDomainEvent;
            """;

        var diagnostics = await GetStoreDiagnosticsAsync(source);

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].Id).IsEqualTo(DiagnosticId);
        await Assert.That(diagnostics[0].GetMessage()).Contains("OrphanEvent");
    }

    [Test, Timeout(30000)]
    public async Task Analyzer_Silent_WhenBaseRegistersDerived(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        // The one correct registration form: [PicoDerivedType] on the base, nothing on the derived.
        // A false positive here would make the diagnostic useless in every real project.
        const string source = """
            using PicoActor.Abs;
            using PicoSerDe.Core;

            [PicoDerivedType(typeof(RegisteredDerived), "reg-derived")]
            public abstract record RegisteredBase : IDomainEvent;

            public sealed record RegisteredDerived(int Delta) : RegisteredBase;
            """;

        var diagnostics = await GetStoreDiagnosticsAsync(source);

        await Assert.That(diagnostics.Length).IsEqualTo(0);
    }

    [Test, Timeout(30000)]
    public async Task Analyzer_Reports_PicoSerializableOnlyDerived(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        // [PicoSerializable] is NOT an escape hatch: it registers the type itself, never the base, so
        // the store still has no discriminator to write the line with.
        const string source = """
            using PicoActor.Abs;
            using PicoSerDe.Core;

            public abstract record SerializableBase : IDomainEvent;

            [PicoSerializable]
            public sealed record SerializableOnlyDerived(int Delta) : SerializableBase;
            """;

        var diagnostics = await GetStoreDiagnosticsAsync(source);

        await Assert.That(diagnostics.Length).IsEqualTo(1);
        await Assert.That(diagnostics[0].Id).IsEqualTo(DiagnosticId);
        await Assert.That(diagnostics[0].GetMessage()).Contains("SerializableOnlyDerived");
    }

    /// <summary>
    /// One compilation per test: TPA plus the three assemblies whose metadata the source needs — the
    /// event contract, its PicoMediator base interface, and the real [PicoDerivedType] attribute.
    /// </summary>
    private static CSharpCompilation CreateCompilation(string source)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            assemblyName: "AnalyzerProbe",
            syntaxTrees: [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        // A source error would silently make a "zero diagnostics" assertion vacuous, so it fails here.
        var errors = compilation
            .GetDiagnostics()
            .Where(static d => d.Severity == DiagnosticSeverity.Error)
            .Select(static d => d.ToString())
            .ToArray();

        if (errors.Length > 0)
            throw new InvalidOperationException(
                "probe compilation must be error-free:"
                    + Environment.NewLine
                    + string.Join(Environment.NewLine, errors)
            );

        return compilation;
    }

    private static MetadataReference[] GetMetadataReferences()
    {
        var trustedPlatformAssemblies = (
            (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
        )!.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries);

        var references = trustedPlatformAssemblies
            .Select(static path => MetadataReference.CreateFromFile(path))
            .ToList();

        // AppContext.BaseDirectory, not Assembly.Location: the latter is empty under
        // single-file/AOT packaging and trips IL3000 once the AOT analyzer is on.
        foreach (
            var name in new[] { "PicoActor.Abs.dll", "PicoMediator.Abs.dll", "PicoSerDe.Core.dll" }
        )
            references.Add(
                MetadataReference.CreateFromFile(Path.Combine(AppContext.BaseDirectory, name))
            );

        return [.. references];
    }

    private static async Task<ImmutableArray<Diagnostic>> GetStoreDiagnosticsAsync(string source)
    {
        var compilation = CreateCompilation(source);
        var withAnalyzers = compilation.WithAnalyzers(
            ImmutableArray.Create<DiagnosticAnalyzer>(new UnregisteredDomainEventAnalyzer())
        );
        var diagnostics = await withAnalyzers.GetAnalyzerDiagnosticsAsync();

        return [.. diagnostics.Where(static d => d.Id == DiagnosticId)];
    }
}
