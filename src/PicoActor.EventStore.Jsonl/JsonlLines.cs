namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The ONE line rule of this store, shared by the count scan, the cursor positioning and the loader
/// so the three can never disagree: a line ends at a newline (and only at a newline), a blank line is
/// one whose bytes are all ASCII space, tab or carriage return, and the bytes after the last newline
/// are a TORN TAIL — the only shape a process crash leaves, never an event.
/// </summary>
internal static class JsonlLines
{
    internal const byte NewLine = (byte)'\n';

    /// <summary>True when the line carries no content (spaces, tabs and carriage returns only).</summary>
    internal static bool IsBlank(ReadOnlySpan<byte> line)
    {
        foreach (var value in line)
        {
            if (!IsBlankByte(value))
                return false;
        }

        return true;
    }

    /// <summary>The byte form of the blank-line rule, for the scan that classifies without copying.</summary>
    internal static bool IsBlankByte(byte value) => value is (byte)' ' or (byte)'\t' or (byte)'\r';
}
