namespace PicoActor.Abs;

/// <summary>
/// Diagnostic token passed to <see cref="Actor.UnhandledErrorHandler"/> when the
/// post-initialization lifecycle hook throws. Not a real command and never dispatched: the
/// hook runs outside any message turn, so the handler (typed on an <see cref="ICommand"/>)
/// needs an explicit stand-in instead of null or an unrelated command.
/// </summary>
internal sealed class InitializationHookCommand : ICommand
{
    public static readonly InitializationHookCommand Instance = new();

    private InitializationHookCommand() { }

    public override string ToString() => nameof(InitializationHookCommand);
}
