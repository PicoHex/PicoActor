namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// One aggregate's append entry: the gate that makes "check the version, then write" atomic for a
/// single store instance, plus the bookkeeping the next append reads instead of the file — the cached
/// event count and the file length it was counted at.
/// <para>
/// The length is the WITNESS: a write after an external modification is refused (a forked history is
/// worse than a failed append), and the only recovery is a re-seed. An entry that is dropped by the
/// table's bound costs one re-scan on the next append, never correctness.
/// </para>
/// </summary>
internal sealed class JsonlLockEntry
{
    internal readonly SemaphoreSlim Gate = new(1, 1);

    /// <summary>Holders plus waiters; guarded by the owning table, never by this entry.</summary>
    internal int Refs;

    /// <summary>True once <see cref="Count"/>/<see cref="Length"/> describe the file.</summary>
    internal bool Seeded;

    /// <summary>Event count at the observed length (the version an append is checked against).</summary>
    internal ulong Count;

    /// <summary>File length at which <see cref="Count"/> was observed.</summary>
    internal long Length;

    /// <summary>Describe the file as seen: the count and the length it was counted at.</summary>
    internal void Seed(ulong count, long length)
    {
        Seeded = true;
        Count = count;
        Length = length;
    }

    /// <summary>Forget the observation, so the next append re-scans (a failed write invalidates).</summary>
    internal void Invalidate()
    {
        Seeded = false;
        Count = 0;
        Length = 0;
    }
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
