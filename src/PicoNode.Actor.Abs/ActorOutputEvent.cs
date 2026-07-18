namespace PicoNode.Actor.Abs;

/// <summary>
/// Outbound event sent through an Actor's OutputChannel.
/// NOT a domain event (IDomainEvent) — it does not change state and is not persisted.
/// It is a fire-and-forget notification to external subscribers (SSE, monitoring, etc.).
/// Symmetric concept to the Mailbox: Mailbox receives commands, OutputChannel broadcasts events.
/// </summary>
public sealed record ActorOutputEvent(
    string Type,
    string? Data,
    string? ToolCallId = null,
    string? ToolName = null,
    string? TurnId = null
);
