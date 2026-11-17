namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// How a stream file is NAMED in the store's root directory. The content format is identical in both
/// modes — only the file name differs — so every directory must be read with the layout it was WRITTEN
/// with: a mixed directory is diagnosed rather than merged, and a directory of the other layout raises
/// instead of enumerating as empty.
/// </summary>
public enum FileNameFormat
{
    /// <summary>
    /// <c>&lt;id:D&gt;.jsonl</c> — the default and the historical layout. The type is not part of the
    /// name, so <c>ListAggregateIdsAsync</c> must open every file to
    /// read its first event (one open per stream).
    /// </summary>
    IdOnly = 0,

    /// <summary>
    /// <c>&lt;type&gt;-&lt;id:D&gt;.jsonl</c>, where <c>{type}</c> is the CONCRETE type name of the
    /// stream's first persisted event (a generic type keeps its backtick arity, exactly as
    /// <see cref="Type.Name"/> reports it). The type is carried by the name, so an enumeration resolves
    /// the aggregate ids from file names alone — zero opens. The prefix is fixed when the stream is
    /// created; the store resolves an id's prefix through an <c>id → type</c> map and one filename pass
    /// on a miss.
    /// </summary>
    TypePrefix = 1,
}
