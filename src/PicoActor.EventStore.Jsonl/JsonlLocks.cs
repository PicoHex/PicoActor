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

    /// <summary>Last use, as a monotonic tick of the owning table — the LRU key.</summary>
    internal long LastUsed;

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
/// The per-aggregate gate table: BOUNDED (a long-lived host touches thousands of aggregates, and one
/// live semaphore per aggregate forever is a leak) and least-recently-used, so the entries that stay are
/// the aggregates that are actually being appended to.
/// <para>
/// One table lock guards membership, every entry's reference count and the LRU tick; it is held only
/// for that arithmetic — never across a gate wait, so a slow append cannot block an unrelated
/// aggregate. An entry is evictable exactly when nobody holds and nobody waits (<c>Refs == 0</c>), which
/// is why the reference is taken BEFORE the wait: a releaser can then never drop an entry a fresh
/// acquirer is about to wait on. Eviction costs one re-scan on that aggregate's next append; it never
/// costs correctness.
/// </para>
/// </summary>
internal sealed class JsonlLockTable(int capacity)
{
    private readonly Dictionary<Guid, JsonlLockEntry> _entries = new();
    private readonly Lock _table = new();
    private long _tick;

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
            var tick = ++_tick;

            if (_entries.TryGetValue(actorId, out var existing))
            {
                existing.Refs++;
                existing.LastUsed = tick;
                return existing;
            }

            // Make room only when the table is full AND an idle entry exists: an entry someone is
            // inside (or waiting on) is never dropped, so a burst of concurrent appends may briefly
            // exceed the bound rather than evict a live gate.
            if (_entries.Count >= capacity)
                EvictLeastRecentlyUsedIdleEntry();

            var entry = new JsonlLockEntry { Refs = 1, LastUsed = tick };
            _entries[actorId] = entry;
            return entry;
        }
    }

    /// <summary>Drop one reference; the entry becomes evictable, and its cache dies with it.</summary>
    internal void Release(JsonlLockEntry entry)
    {
        lock (_table)
        {
            entry.Refs--;
            entry.LastUsed = ++_tick;
        }
    }

    private void EvictLeastRecentlyUsedIdleEntry()
    {
        Guid? victim = null;
        var oldest = long.MaxValue;

        foreach (var (actorId, entry) in _entries)
        {
            if (entry.Refs != 0 || entry.LastUsed >= oldest)
                continue;

            oldest = entry.LastUsed;
            victim = actorId;
        }

        if (victim is { } id)
            _entries.Remove(id);
    }
}
