using Microsoft.CodeAnalysis;

namespace PicoActor.Gen;

[Generator(LanguageNames.CSharp)]
public sealed class PlaceholderGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context) { }
}
