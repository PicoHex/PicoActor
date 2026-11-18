namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// THIS TYPE IS A GENERATION POINT, NOT DEAD CODE — nothing calls it, nothing instantiates it, and
/// deleting it breaks every saga stream this package is asked to write.
/// <para>
/// It exists because PicoSerDe is CALL-SITE driven: its candidates are invocations matched by METHOD
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
/// The two attributes below are that generation point, and the registration they produce is
/// process-wide (PicoSerDe's registry is a static keyed by format and type, installed by the generated
/// module initializer), so every consumer of this package gets both serializers with no wiring of its
/// own — the framework owns its own events' wire format.
/// </para>
/// <para>
/// The DECLARATIVE form is the one that works on this band: PicoSerDe 2026.14's attribute pipeline
/// reads a positional record's primary constructor (the 2026.13 pipeline extracted members only and
/// emitted <c>new SagaCompleted()</c> plus init-only assignments, i.e. CS7036 and CS8852 on exactly
/// these two events). The equivalent closed call sites
/// (<c>JsonSerializer.SerializeToUtf8Bytes(new SagaCompleted(null))</c> and the matching
/// <c>Deserialize</c>) would produce the same registrations, but they would have to live in an
/// unreachable method, and a method whose only purpose is to be read by the compiler is a mystery to
/// every future reader — the attribute says what it means.
/// </para>
/// <para>
/// <para>
/// <see cref="PicoJsonSerializableAttribute"/> (JSON only) rather than the cross-format
/// <c>GenerateSerializer</c>: this store writes JSON lines and nothing else, so asking the other four
/// format generators for code would be dead weight — and it keeps the attribute in the package this
/// project already references, instead of leaning on <c>PicoSerDe.Core</c> arriving transitively.
/// </para>
/// <c>Bnd0_FrameworkEventSerializer_IsGenerated</c> is the tripwire: it observes the registration from
/// another assembly and goes red the moment this generation point disappears.
/// </para>
/// </summary>
[PicoJsonSerializable(typeof(SagaCompleted))]
[PicoJsonSerializable(typeof(SagaFailed))]
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
}
