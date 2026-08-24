namespace PicoActor.Abs;

/// <summary>
/// Outbound event sent through an Actor's OutputChannel.
/// NOT a domain event (IDomainEvent) — it does not change state and is not persisted.
/// It is a fire-and-forget notification to external subscribers (SSE, monitoring, etc.).
/// Symmetric concept to the Mailbox: Mailbox receives commands, OutputChannel broadcasts events.
/// </summary>
/// <remarks>
/// <paramref name="TurnId"/> names a caller-side correlation concept (agent turns);
/// it is opaque to the framework — pure pass-through payload.
/// </remarks>
public sealed record ActorOutputEvent(string Type, string? Data, string? TurnId = null);
