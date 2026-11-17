using PicoSerDe.Core;

namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>An event that does not belong to the store's stream type — the ArgumentException probe.</summary>
public sealed record ForeignEvent(int Seq) : IDomainEvent;

/// <summary>A base whose registration is INCOMPLETE: only one of its two concrete events is declared.</summary>
[PicoDerivedType(typeof(RegisteredPartialEvent), "partial-ok")]
public abstract record PartialEventBase : IDomainEvent;

public sealed record RegisteredPartialEvent(int Delta) : PartialEventBase;

/// <summary>Derived, never registered — the compile-clean, first-append-fatal shape.</summary>
public sealed record UnregisteredPartialEvent(int Delta) : PartialEventBase;

/// <summary>
/// A base with NO registration at all: PicoJetson generates nothing for it, so the serializer lookup
/// fails with the "no serializer/deserializer registered" InvalidOperationException.
/// </summary>
public abstract record UnregisteredEventBase : IDomainEvent;

public sealed record UnregisteredEventImpl(int Delta) : UnregisteredEventBase;
