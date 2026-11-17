namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The two discriminators the framework's OWN events are written with. They are the tags a production
/// corpus already carries, so they are permanent wire format from this release: one tag per type, no
/// alias, no migration, no rename path.
/// <para>
/// ONE owner for the literals: the store's constructor seeds them (D1) and the package's serializer
/// marker (D2b) names the same types. Neither restates the strings, and no consumer is expected to type
/// them by hand — a consumer that wants its own tag simply binds the type before constructing the store,
/// and the default is then never applied.
/// </para>
/// </summary>
internal static class FrameworkEventTags
{
    /// <summary><c>PicoActor.Abs.SagaCompleted</c>'s tag.</summary>
    internal const string SagaCompleted = "PicoActorSagaCompleted";

    /// <summary><c>PicoActor.Abs.SagaFailed</c>'s tag.</summary>
    internal const string SagaFailed = "PicoActorSagaFailed";
}
