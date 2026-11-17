namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// THIS TYPE IS A GENERATION POINT, NOT DEAD CODE — nothing calls it, nothing instantiates it, and
/// deleting it breaks every saga stream this package is asked to write.
/// <para>
/// It exists because PicoJetson is CALL-SITE driven: its candidates are invocations matched by METHOD
/// NAME (<c>Serialize</c>, <c>SerializeToUtf8Bytes</c>, <c>Deserialize</c>, …) plus attributes naming a
/// type — and a generic invocation generates nothing for its type arguments. The store is generic by
/// construction (<see cref="JsonlEventStore{TEventBase}"/> serializes through <c>TEventBase</c>), so
/// no call site in it can ever name a concrete event, and the framework's own two saga terminal events
/// (<see cref="SagaCompleted"/>, <see cref="SagaFailed"/>, declared in <c>PicoActor.Abs</c>) would have
/// NO serializer anywhere in the process: the first saga completion written to a durable store fails
/// with <c>InvalidOperationException: No serializer/deserializer registered for
/// PicoActor.Abs.SagaCompleted</c>.
/// </para>
/// <para>
/// The closed call sites below are that generation point. The registration they produce is
/// process-wide (PicoSerDe's registry is a static keyed by format and type, installed by the generated
/// module initializer), so every consumer of this package gets both serializers with no wiring of its
/// own — the framework owns its own events' wire format.
/// </para>
/// <para>
/// Why closed call sites and not <c>[GenerateSerializer(typeof(SagaCompleted))]</c>: the attribute
/// pipelines all share <c>GenInfrastructure.TransformTypeSymbol</c>, which extracts members only and
/// never reads a record's primary constructor, so on a positional record they emit
/// <c>new SagaCompleted()</c> plus init-only assignments and the package stops compiling — CS7036 and
/// CS8852, observed on these two events and reproduced with a record declared in this very assembly.
/// The invocation pipeline does read the record constructor and emits a deserializer that constructs
/// correctly, which is why the call-site form is the one that works.
/// </para>
/// <para>
/// <c>Bnd0_FrameworkEventSerializer_IsGenerated</c> is the tripwire: it observes the registration from
/// another assembly and goes red the moment this generation point disappears.
/// </para>
/// </summary>
internal static class FrameworkEventSerializers
{
    /// <summary>
    /// The canonical wire tag of <see cref="SagaCompleted"/>, as <see cref="FrameworkEventTags"/> spells
    /// it. An ALIAS, not a copy: <see cref="FrameworkEventTags"/> is the one owner of both literals, so
    /// this name and the store's seed are the same string by construction and cannot drift.
    /// </summary>
    public const string SagaCompletedTag = FrameworkEventTags.SagaCompleted;

    /// <summary>
    /// The canonical wire tag of <see cref="SagaFailed"/>, as <see cref="FrameworkEventTags"/> spells it.
    /// </summary>
    public const string SagaFailedTag = FrameworkEventTags.SagaFailed;

    /// <summary>
    /// The generation point itself. Exists to be READ BY THE COMPILER, never to run: the four closed
    /// call sites name the two framework events with a concrete type argument, which is the only shape
    /// PicoJetson generates a serializer + deserializer from. The bodies are unreachable by design.
    /// </summary>
    internal static void Generate()
    {
        // SerializeToUtf8Bytes<SagaCompleted> / <SagaFailed>: the writer half.
        _ = JsonSerializer.SerializeToUtf8Bytes(new SagaCompleted(null));
        _ = JsonSerializer.SerializeToUtf8Bytes(new SagaFailed(string.Empty));

        // Deserialize<SagaCompleted> / <SagaFailed>: the reader half (the store's read path needs it).
        _ = JsonSerializer.Deserialize<SagaCompleted>(ReadOnlySpan<byte>.Empty);
        _ = JsonSerializer.Deserialize<SagaFailed>(ReadOnlySpan<byte>.Empty);
    }
}
