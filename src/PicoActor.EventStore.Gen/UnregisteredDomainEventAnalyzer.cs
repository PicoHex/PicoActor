namespace PicoActor.EventStore.Gen;

/// <summary>
/// Flags a concrete domain event that no polymorphic registration covers — the compile-clean,
/// first-append-fatal shape: PicoJetson generates one serializer per registration site, so an event
/// that is not listed by a <c>[PicoDerivedType]</c> on its base has no discriminator to be written
/// with, and the failure surfaces only when the store is first asked to write it.
/// <para>
/// The predicate is deliberately local — it walks the candidate's own base chain and asks whether any
/// of those declarations lists it. A global scan of every referenced assembly's attributes would cost
/// the whole metadata graph on every keystroke to answer the same question.
/// </para>
/// <para>
/// What is NOT statically detectable (the runtime wrap still covers it): a type registered under one
/// base but stored through another, duplicate discriminators, and late <c>[ModuleInitializer]</c>
/// registrations from other assemblies.
/// </para>
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class UnregisteredDomainEventAnalyzer : DiagnosticAnalyzer
{
    /// <summary>The diagnostic id consumers promote (or suppress) in their own projects.</summary>
    public const string DiagnosticId = "PICOSTORE001";

    /// <summary>The event contract, matched by metadata name — see <see cref="IsContract"/>.</summary>
    private const string DomainEventNamespace = "PicoActor.Abs";
    private const string DomainEventMetadataName = "IDomainEvent";

    /// <summary>The registration attribute, matched by metadata name (it lives in PicoSerDe.Core).</summary>
    private const string DerivedTypeAttributeNamespace = "PicoSerDe.Core";
    private const string DerivedTypeAttributeMetadataName = "PicoDerivedTypeAttribute";

    /// <summary>
    /// Default severity <see cref="DiagnosticSeverity.Info"/>: the declaration is legal C# and a host
    /// that never touches a durable store is unaffected, so this must not fail a build by default. A
    /// project that does ship a store promotes it through its own rule configuration.
    /// </summary>
    private static readonly DiagnosticDescriptor UnregisteredDomainEvent = new(
        id: DiagnosticId,
        title: "Domain event is not registered for polymorphic serialization",
        messageFormat: "Domain event '{0}' is not listed by any [PicoDerivedType] on its base type, so a JSONL event store cannot serialize it; declare [PicoDerivedType(typeof({0}), \"tag\")] on the base event type",
        category: "PicoActor.EventStore.Gen",
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: true
    );

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(UnregisteredDomainEvent);

    public override void Initialize(AnalysisContext context)
    {
        context.EnableConcurrentExecution();
        // Generated trees are the generator's business: a type it emits is not a declaration the
        // consumer can put an attribute on.
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        if (context.Symbol is not INamedTypeSymbol type)
            return;

        // Only this compilation's own declarations: a referenced assembly's events were registered (or
        // not) where they were compiled, and this analyzer never ran on that build.
        if (
            !SymbolEqualityComparer.Default.Equals(
                type.ContainingAssembly,
                context.Compilation.Assembly
            )
        )
            return;

        if (!IsCandidate(type) || IsRegistered(type))
            return;

        var location = type.Locations.Length > 0 ? type.Locations[0] : Location.None;
        context.ReportDiagnostic(Diagnostic.Create(UnregisteredDomainEvent, location, type.Name));
    }

    /// <summary>
    /// The shape a store can actually be handed: a concrete class or struct (a record included) that
    /// implements the event contract. Interfaces and abstract types are never instantiated, so they are
    /// never serialized as an event — that is exactly why the registration base carries the attributes
    /// itself and is normally abstract.
    /// </summary>
    private static bool IsCandidate(INamedTypeSymbol type)
    {
        if (type.TypeKind is not (TypeKind.Class or TypeKind.Struct))
            return false;
        if (type.IsAbstract || type.IsStatic)
            return false;

        foreach (var @interface in type.AllInterfaces)
            if (IsContract(@interface))
                return true;

        return false;
    }

    /// <summary>
    /// Registration is one-directional and lives on the BASE: <c>[PicoDerivedType(typeof(T), "tag")]</c>
    /// on the type T derives from. Walking up from the candidate is therefore the whole predicate, and
    /// it reads source and metadata attributes alike. <c>[PicoSerializable]</c> on the candidate itself
    /// is not an escape hatch — it registers the candidate, never the base, so it is simply ignored
    /// here.
    /// </summary>
    private static bool IsRegistered(INamedTypeSymbol type)
    {
        var original = type.OriginalDefinition;

        for (var current = type; current is not null; current = current.BaseType)
        {
            foreach (var attribute in current.GetAttributes())
            {
                if (attribute.AttributeClass is not { } attributeClass)
                    continue;
                if (!IsDerivedTypeAttribute(attributeClass))
                    continue;

                foreach (var argument in attribute.ConstructorArguments)
                {
                    // The first argument is typeof(TDerived); a malformed declaration (an error type,
                    // or a non-type argument) simply cannot cover anything.
                    if (argument.Kind != TypedConstantKind.Type)
                        continue;
                    if (argument.Value is not ITypeSymbol registered)
                        continue;
                    // OriginalDefinition: a registration of a closed generic also covers the generic
                    // declaration, which is the only thing a source declaration can be compared to.
                    if (
                        SymbolEqualityComparer.Default.Equals(
                            registered.OriginalDefinition,
                            original
                        )
                    )
                        return true;
                }
            }
        }

        return false;
    }

    private static bool IsDerivedTypeAttribute(INamedTypeSymbol attributeClass) =>
        attributeClass.MetadataName == DerivedTypeAttributeMetadataName
        && attributeClass.ContainingNamespace?.ToDisplayString() == DerivedTypeAttributeNamespace;

    /// <summary>
    /// Metadata-name comparison, not a symbol reference: this analyzer is netstandard2.0 and cannot
    /// reference the class library whose contract it checks. The contract's identity (namespace +
    /// name) is public and stable, and the metadata name is what a compiled consumer actually sees.
    /// </summary>
    private static bool IsContract(INamedTypeSymbol candidate) =>
        candidate.MetadataName == DomainEventMetadataName
        && candidate.ContainingNamespace?.ToDisplayString() == DomainEventNamespace;
}
