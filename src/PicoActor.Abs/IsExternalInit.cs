namespace System.Runtime.CompilerServices;

/// <summary>
/// Polyfill for the <c>init</c> keyword accessor on netstandard2.0.
/// This type is normally defined by the compiler in .NET 5+, but must be
/// provided manually for netstandard2.0 targets.
/// </summary>
internal class IsExternalInit { }
