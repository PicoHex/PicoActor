namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// One aggregate's append gate. Append is the only operation that mutates a stream, so the gate is
/// what makes "check the version, then write" atomic for a single store instance.
/// </summary>
internal sealed class JsonlLockEntry
{
    internal readonly SemaphoreSlim Gate = new(1, 1);

    /// <summary>Holders plus waiters; guarded by the owning table, never by this entry.</summary>
    internal int Refs;
}

/// <summary>
/// The per-aggregate gate table. One entry per stream ever appended to, guarded by ONE table lock that
/// is held only for dictionary arithmetic — never across a gate wait, so a slow append cannot block an
/// unrelated aggregate.
/// </summary>
internal sealed class JsonlLockTable
{
    private readonly Dictionary<Guid, JsonlLockEntry> _entries = new();
    private readonly Lock _table = new();

    /// <summary>Live entries — the observable side of the bound.</summary>
    internal int Count
    {
        get
        {
            lock (_table)
                return _entries.Count;
        }
    }

    /// <summary>Take the gate entry for <paramref name="actorId"/>, ref-counted before any wait.</summary>
    internal JsonlLockEntry Acquire(Guid actorId)
    {
        lock (_table)
        {
            if (!_entries.TryGetValue(actorId, out var entry))
            {
                entry = new JsonlLockEntry();
                _entries[actorId] = entry;
            }

            entry.Refs++;
            return entry;
        }
    }

    /// <summary>Drop one reference; the entry itself is reclaimed by the bound (see the release path).</summary>
    internal void Release(Guid actorId, JsonlLockEntry entry)
    {
        lock (_table)
        {
            entry.Refs--;
        }
    }
}
