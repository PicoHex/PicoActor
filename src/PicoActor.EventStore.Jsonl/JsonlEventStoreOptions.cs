namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// Tunables of <see cref="JsonlEventStore{TEventBase}"/>.
/// </summary>
public sealed record JsonlEventStoreOptions
{
    /// <summary>
    /// fsync every append. Default <c>false</c>: the bytes are flushed to the OS (so another reader
    /// sees the event immediately) but not to the platter. The documented cost of leaving it off is
    /// that a power loss can leave a TERMINATED garbage line, which the store treats as corruption
    /// and never repairs — an operator removes the line.
    /// </summary>
    public bool FlushToDisk { get; init; }

    /// <summary>
    /// Upper bound of the per-aggregate gate table. Must be greater than zero. An evicted entry costs
    /// one re-scan on that aggregate's next append, never correctness.
    /// </summary>
    public int MaxAggregateLocks { get; init; } = 1024;

    /// <summary>
    /// The event types this store may write and read IN ADDITION to its generic argument, each bound to
    /// the tag its lines carry in <c>$type</c>. EMPTY here: the store's constructor is the only owner of
    /// the framework defaults, so there is exactly one seeding mechanism and
    /// <see cref="EventTypeBindings.FrameworkDefaultCount"/> says how many of them were applied.
    /// <para>
    /// Bind BEFORE constructing the store: the constructor freezes this table and copies it, so a later
    /// <see cref="EventTypeBindings.Add{T}"/> is refused rather than silently ignored. One store per
    /// options instance remains the supported shape.
    /// </para>
    /// </summary>
    public EventTypeBindings EventTypeBindings { get; } = new();

    /// <summary>
    /// How a stream FILE is named in the store's root directory. Default
    /// <see cref="FileNameFormat.IdOnly"/> — <c>&lt;id:D&gt;.jsonl</c>, the historical layout, which
    /// this store reproduces byte-for-byte (names, enumeration, counts).
    /// <para>
    /// <see cref="FileNameFormat.TypePrefix"/> names a stream <c>&lt;type&gt;-&lt;id:D&gt;.jsonl</c>
    /// after its first event's concrete type, which lets an enumeration resolve the aggregate ids from
    /// the names alone (zero file opens). The two layouts share the content format, so a directory can
    /// be pointed at either — but never both: the store diagnoses a directory whose streams are all in
    /// the other layout instead of enumerating nothing.
    /// </para>
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The assigned value is not a defined <see cref="FileNameFormat"/>: a config typo must never fall
    /// back silently to <see cref="FileNameFormat.IdOnly"/>.
    /// </exception>
    public FileNameFormat FileNameFormat
    {
        get => _fileNameFormat;
        init
        {
            if (!Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(
                    nameof(FileNameFormat),
                    value,
                    "FileNameFormat must be a defined FileNameFormat value."
                );

            _fileNameFormat = value;
        }
    }

    /// <summary>
    /// The store's logger, independent of any DI <c>ILoggerFactory</c>: a host that wants the store's
    /// warnings in its own log passes the same logger explicitly. Null is silent.
    /// </summary>
    public ILogger? Logger { get; init; }

    private readonly FileNameFormat _fileNameFormat = FileNameFormat.IdOnly;
}
