using PicoActor.Abs;
using PicoSerDe.Core;

namespace PicoActor.TestSupport;

/// <summary>
/// The polymorphic seed every store conformance suite reuses.
/// <para>
/// The registration form is fixed: <see cref="ConformanceEvent"/> carries
/// <c>[PicoDerivedType]</c> for each concrete event and NOTHING else — no
/// <c>[PicoSerializable]</c> on the base, no <c>[PicoPolymorphic]</c> (the discriminator
/// property name is irrelevant to a store). That is the ONLY form a durable store can rely on:
/// PicoJetson's generator ignores type parameters, so the serializer is generated in the
/// assembly that declares the base, and a derived type that carries only
/// <c>[PicoSerializable]</c> registers itself — never the base — and fails on the first write.
/// </para>
/// </summary>
[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]
public abstract record ConformanceEvent : IDomainEvent;

/// <summary>
/// The one concrete conformance event: a delta, so value equality lets an assertion compare a
/// deserialized event with the instance that was appended. The name is load-bearing —
/// <c>RebuildReaderPathTests</c> looks an aggregate up by <c>nameof(SSnapshotAdded)</c>.
/// </summary>
public sealed record SSnapshotAdded(int Delta) : ConformanceEvent;
