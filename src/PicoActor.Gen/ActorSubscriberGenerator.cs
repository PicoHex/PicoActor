namespace PicoActor.Gen;

/// <summary>
/// Declare-and-subscribe for PicoActor: scans closed, non-abstract
/// IDomainEventSubscriber&lt;TEvent&gt; implementations and emits a per-assembly
/// configurator (id "pico-actor::&lt;assembly&gt;") into MediatorAutoSubscriptionRegistry.
/// The configurator registers each handler (append semantics — multiple handlers
/// per event type are supported) and one envelope bridge per distinct event type.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class ActorSubscriberGenerator : IIncrementalGenerator
{
    /// <summary>
    /// A subscriber declaration that cannot be constructed (no accessible instance
    /// constructor) must fail loudly at build time — emitting `new T()` for it would
    /// surface as CS0122/CS7036 inside generated code with no hint about the cause.
    /// </summary>
    private static readonly DiagnosticDescriptor UnregistrableSubscriber = new(
        id: "PICA001",
        title: "Domain-event subscriber is not registrable",
        messageFormat: "IDomainEventSubscriber implementation '{0}' has no accessible (public or internal) instance constructor and cannot be registered; declare one or remove the implementation",
        category: "PicoActor.Gen",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    /// <summary>
    /// A subscriber shape the generator cannot emit — generic (no closed type to
    /// instantiate) or file-local (unreachable from a generated tree) — used to be dropped
    /// silently, so declare-and-subscribe just never fired. Warning, not Error: the
    /// declaration is legal C# and a manual container.Register(...) makes it work, so
    /// failing the build would break consumers that register by hand.
    /// </summary>
    private static readonly DiagnosticDescriptor UnregisterableSubscriberShape = new(
        id: "PICA002",
        title: "Domain-event subscriber is not auto-registered",
        messageFormat: "IDomainEventSubscriber implementation '{0}' will not be auto-registered ({1}); register it manually in the container",
        category: "PicoActor.Gen",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true
    );

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var subscriberDeclarations = context
            .SyntaxProvider.CreateSyntaxProvider(
                predicate: static (node, _) =>
                    node is TypeDeclarationSyntax t && t.BaseList?.Types.Count > 0,
                transform: static (ctx, ct) => GetSubscriberInfos(ctx, ct)
            )
            .Where(static x => x.Length > 0)
            .SelectMany(static (x, _) => x);

        // Only the assembly name is needed from the compilation: projecting it to a string
        // keeps the output node cacheable. Feeding CompilationProvider straight into
        // RegisterSourceOutput would re-run generation on every edit (a Compilation
        // instance changes on every keystroke).
        var assemblyNameProvider = context.CompilationProvider.Select(
            static (compilation, _) => compilation.AssemblyName ?? "Unknown"
        );

        // The batch node marks itself modified whenever its inputs change (any edit inside
        // any file), even when the collected subscribers are byte-for-byte the same. Compare
        // the collected set structurally so an unrelated edit leaves the source output cached.
        var subscriberSummary = subscriberDeclarations
            .Collect()
            .WithComparer(SubscriberSetComparer.Instance);

        context.RegisterSourceOutput(
            assemblyNameProvider.Combine(subscriberSummary),
            static (spc, pair) => GenerateRegistrations(spc, pair.Left, pair.Right)
        );
    }

    private sealed class SubscriberInfo
    {
        private SubscriberInfo(
            INamedTypeSymbol implementationSymbol,
            ITypeSymbol? eventTypeSymbol,
            string eventTypeFqn,
            string implementationType,
            Location? location,
            ImmutableArray<string> constructorParameterTypes,
            bool hasAccessibleConstructor,
            string? skipReason
        )
        {
            ImplementationSymbol = implementationSymbol;
            EventTypeSymbol = eventTypeSymbol;
            EventTypeFqn = eventTypeFqn;
            ImplementationType = implementationType;
            Location = location;
            ConstructorParameterTypes = constructorParameterTypes;
            HasAccessibleConstructor = hasAccessibleConstructor;
            SkipReason = skipReason;
        }

        /// <summary>One registration: <paramref name="implementationSymbol"/> for one event type.</summary>
        public static SubscriberInfo Registration(
            INamedTypeSymbol implementationSymbol,
            ITypeSymbol eventTypeSymbol,
            string eventTypeFqn,
            string implementationType,
            Location? location,
            ImmutableArray<string> constructorParameterTypes,
            bool hasAccessibleConstructor
        ) =>
            new(
                implementationSymbol,
                eventTypeSymbol,
                eventTypeFqn,
                implementationType,
                location,
                constructorParameterTypes,
                hasAccessibleConstructor,
                skipReason: null
            );

        /// <summary>A subscriber declaration the generator must skip and report (PICA002).</summary>
        public static SubscriberInfo Skipped(
            INamedTypeSymbol implementationSymbol,
            string implementationType,
            Location? location,
            string reason
        ) =>
            new(
                implementationSymbol,
                eventTypeSymbol: null,
                eventTypeFqn: string.Empty,
                implementationType,
                location,
                [],
                hasAccessibleConstructor: true,
                skipReason: reason
            );

        /// <summary>
        /// The merged (partial-collapsed) handler symbol. The scan is syntax-driven, so a
        /// type whose base list is spread over several partial declarations is visited once
        /// per declaration while this symbol stays the SAME — it is the identity key.
        /// </summary>
        public INamedTypeSymbol ImplementationSymbol { get; }

        public ITypeSymbol? EventTypeSymbol { get; }
        public string EventTypeFqn { get; }
        public string ImplementationType { get; }
        public Location? Location { get; }
        public ImmutableArray<string> ConstructorParameterTypes { get; }

        /// <summary>Non-null → the shape cannot be emitted at all (PICA002), with the reason.</summary>
        public string? SkipReason { get; }

        /// <summary>False → the declaration cannot be instantiated by generated code (PICA001).</summary>
        public bool HasAccessibleConstructor { get; }

        /// <summary>Structural comparison over the fields that affect generated code.</summary>
        public bool HasSameShapeAs(SubscriberInfo other) =>
            string.Equals(EventTypeFqn, other.EventTypeFqn, StringComparison.Ordinal)
            && string.Equals(ImplementationType, other.ImplementationType, StringComparison.Ordinal)
            && string.Equals(SkipReason, other.SkipReason, StringComparison.Ordinal)
            && HasAccessibleConstructor == other.HasAccessibleConstructor
            && ConstructorParameterTypes.SequenceEqual(
                other.ConstructorParameterTypes,
                StringComparer.Ordinal
            );

        // netstandard2.0 target: no System.HashCode — combine manually
        public int ShapeHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(EventTypeFqn);
                hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(ImplementationType);
                hash =
                    (hash * 31)
                    + (SkipReason is null ? 0 : StringComparer.Ordinal.GetHashCode(SkipReason));
                hash = (hash * 31) + ConstructorParameterTypes.Length;
                hash = (hash * 31) + (HasAccessibleConstructor ? 1 : 0);
                return hash;
            }
        }
    }

    /// <summary>
    /// Identity of one registration: (handler type, event type). Roslyn symbols compare
    /// with <see cref="SymbolEqualityComparer"/>, which is exact — two distinct types that
    /// happen to share a display name (or a sanitized identifier) never collapse.
    /// </summary>
    private readonly struct RegistrationKey : IEquatable<RegistrationKey>
    {
        private readonly INamedTypeSymbol _implementation;
        private readonly ITypeSymbol? _eventType;

        public RegistrationKey(INamedTypeSymbol implementation, ITypeSymbol? eventType)
        {
            _implementation = implementation;
            _eventType = eventType;
        }

        public bool Equals(RegistrationKey other) =>
            SymbolEqualityComparer.Default.Equals(_implementation, other._implementation)
            && SymbolEqualityComparer.Default.Equals(_eventType, other._eventType);

        public override bool Equals(object? obj) => obj is RegistrationKey other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = SymbolEqualityComparer.Default.GetHashCode(_implementation);
                hash =
                    (hash * 397)
                    ^ (
                        _eventType is null
                            ? 0
                            : SymbolEqualityComparer.Default.GetHashCode(_eventType)
                    );
                return hash;
            }
        }
    }

    /// <summary>
    /// Value comparer for the collected subscriber set: Roslyn compares node outputs with
    /// this to decide whether generation must re-run. Without it a changed array instance
    /// (produced on every edit anywhere in the project) invalidates the output node.
    /// </summary>
    private sealed class SubscriberSetComparer : IEqualityComparer<ImmutableArray<SubscriberInfo>>
    {
        public static readonly SubscriberSetComparer Instance = new();

        public bool Equals(ImmutableArray<SubscriberInfo> x, ImmutableArray<SubscriberInfo> y)
        {
            if (x.Length != y.Length)
                return false;
            for (var i = 0; i < x.Length; i++)
                if (!x[i].HasSameShapeAs(y[i]))
                    return false;
            return true;
        }

        public int GetHashCode(ImmutableArray<SubscriberInfo> obj)
        {
            unchecked
            {
                var hash = obj.Length;
                foreach (var item in obj)
                    hash = (hash * 31) + item.ShapeHashCode();
                return hash;
            }
        }
    }

    private static ImmutableArray<SubscriberInfo> GetSubscriberInfos(
        GeneratorSyntaxContext ctx,
        CancellationToken ct
    )
    {
        if (ctx.Node is not TypeDeclarationSyntax typeDecl)
            return [];

        var typeSymbol = ctx.SemanticModel.GetDeclaredSymbol(typeDecl, ct) as INamedTypeSymbol;
        if (typeSymbol is null || typeSymbol.IsAbstract)
            return [];

        var accessibility = typeSymbol.DeclaredAccessibility;
        if (accessibility != Accessibility.Public && accessibility != Accessibility.Internal)
            return [];

        // Cheap probe first: most declarations in a compilation are not subscribers.
        if (!typeSymbol.AllInterfaces.Any(static i => IsDomainEventSubscriberInterface(i)))
            return [];

        var implementationType = typeSymbol.ToDisplayString(
            SymbolDisplayFormat.FullyQualifiedFormat
        );
        var location = typeSymbol.Locations.Length > 0 ? typeSymbol.Locations[0] : null;

        // Shapes whose registration cannot be emitted. Returning [] silently — the previous
        // behavior — made declare-and-subscribe a no-op with nothing in the build log.
        var skipReason = SkipReasonFor(typeSymbol);
        if (skipReason is not null)
            return [SubscriberInfo.Skipped(typeSymbol, implementationType, location, skipReason)];

        var results = ImmutableArray.CreateBuilder<SubscriberInfo>();
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            if (!IsDomainEventSubscriberInterface(iface))
                continue;

            var ctor = typeSymbol
                .InstanceConstructors.Where(c =>
                    !c.IsStatic
                    && c.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal
                )
                // Greediest constructor (DI convention, mirrors PicoDI): a parameterless
                // ctor declared first must not win over the injectable one.
                .OrderByDescending(static c => c.Parameters.Length)
                .FirstOrDefault();
            var ctorParams = ctor is null
                ? []
                : ctor
                    .Parameters.Select(p =>
                        p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
                    )
                    .ToImmutableArray();

            var eventTypeSymbol = iface.TypeArguments[0];
            results.Add(
                SubscriberInfo.Registration(
                    typeSymbol,
                    eventTypeSymbol,
                    eventTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    implementationType,
                    location,
                    ctorParams,
                    ctor is not null
                )
            );
        }

        return results.ToImmutable();
    }

    /// <summary>
    /// Namespace-locked scan: only PicoActor.Abs.IDomainEventSubscriber&lt;TEvent&gt; counts —
    /// a PicoMediator ISubscriber&lt;TEvent&gt; is somebody else's registration.
    /// </summary>
    private static bool IsDomainEventSubscriberInterface(INamedTypeSymbol iface) =>
        iface.IsGenericType
        && iface.ConstructedFrom.ContainingNamespace.ToDisplayString() == "PicoActor.Abs"
        && iface.ConstructedFrom.MetadataName == "IDomainEventSubscriber`1";

    /// <summary>
    /// Why this declaration cannot be turned into generated registration code, or null.
    /// Both reasons are legal C# the generator simply cannot reference from another tree.
    /// </summary>
    private static string? SkipReasonFor(INamedTypeSymbol typeSymbol)
    {
        if (typeSymbol.IsFileLocal)
            return "file-local types cannot be referenced from generated code";

        // Nested inside a generic type counts too: the generated new X(...) could only name
        // an open type, so the registration would not compile.
        for (var current = typeSymbol; current is not null; current = current.ContainingType)
        {
            if (current.IsGenericType)
                return "generic subscriber types cannot be instantiated by the generated factory";
        }

        return null;
    }

    private static void GenerateRegistrations(
        SourceProductionContext context,
        string assemblyName,
        ImmutableArray<SubscriberInfo> subscribers
    )
    {
        if (subscribers.IsDefaultOrEmpty)
            return;

        // The scan is syntax-driven: a partial type with a base list in more than one
        // declaration is visited once per declaration, while GetDeclaredSymbol returns the
        // same MERGED symbol each time. Without this, one handler would be registered twice
        // for the same event type. Dedupe on the symbols themselves, never on strings.
        var seen = new HashSet<RegistrationKey>();
        var distinct = ImmutableArray.CreateBuilder<SubscriberInfo>(subscribers.Length);
        foreach (var s in subscribers)
        {
            if (seen.Add(new RegistrationKey(s.ImplementationSymbol, s.EventTypeSymbol)))
                distinct.Add(s);
        }

        // Unregistrable shapes (no accessible instance constructor) get a loud build-time
        // diagnostic instead of an uncompilable `new T()` factory in consumer builds.
        var registrable = ImmutableArray.CreateBuilder<SubscriberInfo>(subscribers.Length);
        foreach (var s in distinct)
        {
            if (s.SkipReason is { } reason)
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(
                        UnregisterableSubscriberShape,
                        s.Location ?? Location.None,
                        s.ImplementationType,
                        reason
                    )
                );
                continue;
            }

            if (s.HasAccessibleConstructor)
            {
                registrable.Add(s);
                continue;
            }

            context.ReportDiagnostic(
                Diagnostic.Create(
                    UnregistrableSubscriber,
                    s.Location ?? Location.None,
                    s.ImplementationType
                )
            );
        }

        if (registrable.Count == 0)
            return;

        var usable = registrable.ToImmutable();

        var safeAssemblyName = SanitizeIdentifier(assemblyName);
        var className = $"PicoActorSubscriberRegistrations_{safeAssemblyName}";
        var configuratorId = $"pico-actor::{assemblyName}";

        var distinctEventTypes = usable
            .Select(static s => s.EventTypeFqn)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static x => x, StringComparer.Ordinal)
            .ToArray();

        // Bridge class names must stay unique within the generated file: sanitization maps
        // distinct fully-qualified names onto the same identifier (My.Event → globalMy_Event,
        // My_Event → globalMy_Event), which would emit two identically named classes (CS0101).
        // Colliding names get a stable FQN-derived suffix.
        var bridgeNames = BuildBridgeNames(distinctEventTypes);

        var sb = new StringBuilder(8192);
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine("using System.Runtime.CompilerServices;");
        sb.AppendLine();
        sb.AppendLine("namespace PicoActor.Generated;");
        sb.AppendLine();

        foreach (var eventType in distinctEventTypes)
        {
            var bridgeName = bridgeNames[eventType];
            sb.AppendLine(
                $"internal sealed class {bridgeName} : global::PicoMediator.Abs.ISubscriber<global::PicoActor.Abs.DomainEventEnvelope>"
            );
            sb.AppendLine("{");
            sb.AppendLine("    private readonly global::PicoDI.Abs.ISvcScope _scope;");
            sb.AppendLine();
            sb.AppendLine(
                $"    public {bridgeName}(global::PicoDI.Abs.ISvcScope scope) => _scope = scope;"
            );
            sb.AppendLine();
            sb.AppendLine(
                // Fully qualified: the generated file is compiled in the consumer project,
                // so it must not depend on that project's using directives (a consumer with
                // ImplicitUsings disabled has no ambient System.Threading.Tasks).
                "    public async global::System.Threading.Tasks.ValueTask Handle(global::PicoActor.Abs.DomainEventEnvelope envelope, global::System.Threading.CancellationToken ct)"
            );
            sb.AppendLine("    {");
            sb.AppendLine($"        if (envelope.Event is {eventType} e)");
            sb.AppendLine("        {");
            sb.AppendLine(
                $"            if (!_scope.TryGetServices(typeof(global::PicoActor.Abs.IDomainEventSubscriber<{eventType}>), out var rawHandlers))"
            );
            sb.AppendLine("                return;");
            sb.AppendLine("            var sender =");
            sb.AppendLine(
                "                _scope.GetService(typeof(global::PicoActor.Abs.ICommandSender)) as global::PicoActor.Abs.ICommandSender"
            );
            sb.AppendLine("                ?? throw new global::System.InvalidOperationException(");
            sb.AppendLine(
                "                    \"ICommandSender is not registered — call AddPicoActor() before AddPicoMediator().\");"
            );
            sb.AppendLine(
                $"            var typed = new global::PicoActor.Abs.DomainEventEnvelope<{eventType}>(envelope.ActorId, envelope.Version, e);"
            );
            sb.AppendLine(
                "            global::System.Collections.Generic.List<global::System.Exception>? exceptions = null;"
            );
            sb.AppendLine("            foreach (var raw in rawHandlers!)");
            sb.AppendLine("            {");
            sb.AppendLine("                try");
            sb.AppendLine("                {");
            sb.AppendLine(
                $"                    await ((global::PicoActor.Abs.IDomainEventSubscriber<{eventType}>)raw).Handle(typed, sender, ct).ConfigureAwait(false);"
            );
            sb.AppendLine("                }");
            sb.AppendLine("                catch (global::System.Exception ex)");
            sb.AppendLine("                {");
            // Explicit construction, not a collection expression: this file is compiled
            // with the CONSUMER's LangVersion, and `exceptions ??= []` needs C# 12 (CS9058
            // on a consumer pinned to 11). Keep generated code at the C# 11 floor.
            sb.AppendLine(
                "                    exceptions = new global::System.Collections.Generic.List<global::System.Exception>();"
            );
            sb.AppendLine("                    exceptions.Add(ex);");
            sb.AppendLine("                }");
            sb.AppendLine("            }");
            sb.AppendLine("            if (exceptions is { Count: > 0 })");
            sb.AppendLine(
                "                throw new global::System.AggregateException(exceptions);"
            );
            sb.AppendLine("        }");
            sb.AppendLine("    }");
            sb.AppendLine("}");
            sb.AppendLine();
        }

        // internal, not public: [ModuleInitializer] must be reachable from the module that
        // hosts it, nothing else — no reason to widen the consumer's public surface.
        sb.AppendLine($"internal static class {className}");
        sb.AppendLine("{");
        sb.AppendLine("    [ModuleInitializer]");
        sb.AppendLine("    internal static void AutoRegister()");
        sb.AppendLine("    {");
        sb.AppendLine("        global::PicoMediator.MediatorAutoSubscriptionRegistry.Register(");
        sb.AppendLine(
            $"            \"{EscapeStringLiteral(configuratorId)}\", static container => ConfigureGeneratedHandlers(container));"
        );
        sb.AppendLine("    }");
        sb.AppendLine();
        sb.AppendLine(
            "    internal static void ConfigureGeneratedHandlers(global::PicoDI.Abs.ISvcContainer container)"
        );
        sb.AppendLine("    {");
        foreach (var s in usable)
        {
            sb.AppendLine("        container.Register(global::PicoDI.Abs.SvcDescriptor.Create(");
            sb.AppendLine(
                $"            typeof(global::PicoActor.Abs.IDomainEventSubscriber<{s.EventTypeFqn}>),"
            );
            sb.AppendLine($"            {EmitFactory(s)},");
            sb.AppendLine("            global::PicoDI.Abs.SvcLifetime.Transient));");
            sb.AppendLine();
        }
        foreach (var eventType in distinctEventTypes)
        {
            var bridgeName = bridgeNames[eventType];
            sb.AppendLine("        container.Register(global::PicoDI.Abs.SvcDescriptor.Create(");
            sb.AppendLine(
                "            typeof(global::PicoMediator.Abs.ISubscriber<global::PicoActor.Abs.DomainEventEnvelope>),"
            );
            sb.AppendLine($"            static scope => new {bridgeName}(scope),");
            sb.AppendLine("            global::PicoDI.Abs.SvcLifetime.Transient));");
            sb.AppendLine();
        }
        sb.AppendLine("    }");
        sb.AppendLine("}");
        context.AddSource(
            $"PicoActorSubscriberRegistrations.{safeAssemblyName}.g.cs",
            sb.ToString()
        );
    }

    private static string EmitFactory(SubscriberInfo s)
    {
        if (s.ConstructorParameterTypes.IsEmpty)
            return $"static _ => new {s.ImplementationType}()";

        var args = string.Join(
            ", ",
            s.ConstructorParameterTypes.Select(p => $"({p})scope.GetService(typeof({p}))")
        );
        return $"static scope => new {s.ImplementationType}({args})";
    }

    /// <summary>
    /// Bridge class name per distinct event type. Names are derived from the sanitized
    /// fully-qualified name; when two different event types sanitize to the same identifier,
    /// all members of the colliding group get a stable FQN-derived suffix so the generated
    /// file stays compilable (CS0101-free).
    /// </summary>
    private static Dictionary<string, string> BuildBridgeNames(string[] eventTypes)
    {
        var names = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var group in eventTypes.GroupBy(SanitizeIdentifier, StringComparer.Ordinal))
        {
            var collides = group.Count() > 1;
            foreach (var eventType in group)
            {
                var sanitized = SanitizeIdentifier(eventType);
                names[eventType] = collides
                    ? $"PicoActorEnvelopeBridge_{sanitized}_{StableSuffix(eventType)}"
                    : $"PicoActorEnvelopeBridge_{sanitized}";
            }
        }
        return names;
    }

    /// <summary>Deterministic 8-hex-char FNV-1a suffix (never randomized, unlike string.GetHashCode).</summary>
    private static string StableSuffix(string value)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;
        var hash = offsetBasis;
        foreach (var c in value)
        {
            hash ^= c;
            hash *= prime;
        }
        return hash.ToString("x8");
    }

    private static string SanitizeIdentifier(string value) =>
        new(
            value
                .Select(c => c == '.' ? '_' : c)
                .Where(static c => char.IsLetterOrDigit(c) || c == '_')
                .ToArray()
        );

    private static string EscapeStringLiteral(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
