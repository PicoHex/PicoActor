namespace PicoNode.Actor.Abs;

/// <summary>
/// Envelope that wraps a command with an optional TaskCompletionSource for Ask semantics.
/// Not intended for user code — used by the framework and persistence layer.
/// </summary>
public sealed class Envelope
{
    public ICommand Command { get; init; } = null!;
    public TaskCompletionSource<object?>? Tcs { get; init; }
}
