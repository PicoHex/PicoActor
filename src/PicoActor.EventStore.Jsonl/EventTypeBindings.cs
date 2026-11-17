namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// The event types a <see cref="JsonlEventStore{TEventBase}"/> may write and read IN ADDITION to
/// <c>TEventBase</c>, each bound to the tag its lines carry in <c>$type</c>.
/// <para>
/// The table belongs to the options and the STORE COPIES it at construction (D8): a binding added after
/// a store was built can never change what that store writes or reads, and the append hot path reads a
/// frozen structure with no lock and no mutable-dictionary read. The single public entry point is
/// <see cref="JsonlEventStoreOptions.EventTypeBindings"/> — the constructor is internal, because a table
/// floating free of an options instance would be a registration nothing consumes.
/// </para>
/// <para>
/// <see cref="Add{T}"/> registers ROUTING ONLY. It does not generate a serializer: a bound type needs a
/// PicoJetson serializer in some assembly loaded in the process. This package supplies the framework's
/// two terminal events; a consumer's own bound type must be generated in the consumer's assembly (a
/// <c>[GenerateSerializer(typeof(T))]</c> marker on one of its types, or a literal serialize call). A
/// bound type without one fails on its FIRST WRITE, loudly — never silently, and never with a file
/// created.
/// </para>
/// </summary>
public sealed class EventTypeBindings
{
    // ONE gate for three related facts: the append path asks "which tag does this Type splice?", the
    // read path asks "which Type does this tag deserialize into?", and the freeze is the hand-off
    // between the mutable table and the store's copy. A monitor is enough — every operation is a
    // dictionary edit or a copy.
    private readonly object _gate = new();

    private readonly Dictionary<Type, string> _tagByType = new();
    private readonly Dictionary<string, Type> _typeByTag = new(StringComparer.Ordinal);

    private int _frameworkDefaultCount;
    private bool _frozen;
    private string _frozenBy = string.Empty;
    private EventTypeBindingsTable? _table;

    /// <summary>
    /// Internal on purpose: an options instance owns a table, so the only public way to get one is
    /// <see cref="JsonlEventStoreOptions.EventTypeBindings"/>.
    /// </summary>
    internal EventTypeBindings() { }

    /// <summary>
    /// Every binding on this options instance, consumer bindings and applied framework defaults alike.
    /// </summary>
    public int RegisteredCount
    {
        get
        {
            lock (_gate)
                return _tagByType.Count;
        }
    }

    /// <summary>
    /// Framework defaults that were actually APPLIED (0..2). A consumer binding on the same type or the
    /// same tag is never overwritten, so this is the count of seeds that took effect — not the number the
    /// framework ships. Together with <see cref="RegisteredCount"/> it is what a boot check asserts.
    /// </summary>
    public int FrameworkDefaultCount
    {
        get
        {
            lock (_gate)
                return _frameworkDefaultCount;
        }
    }

    /// <summary>
    /// Bind <typeparamref name="T"/> to the discriminator <paramref name="tag"/> it is written with.
    /// This routes the type through this store; it does NOT make the type serializable (see the type
    /// documentation).
    /// </summary>
    /// <typeparam name="T">The bound event type; it must be serializable in some loaded assembly.</typeparam>
    /// <param name="tag">The <c>$type</c> value, spliced verbatim into every line of this type.</param>
    /// <returns>This table, so bindings can be chained.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tag"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="tag"/> is empty, whitespace only, or contains a quote, a backslash or a control
    /// character — each of those would be spliced into the line verbatim and produce invalid JSON that
    /// only surfaces on read-back.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <typeparamref name="T"/> is already bound to a DIFFERENT tag, another type is already bound to
    /// <paramref name="tag"/>, or a store has already been constructed over these options (the table is
    /// frozen). The same type with the same tag is an idempotent no-op.
    /// </exception>
    public EventTypeBindings Add<T>(string tag)
        where T : class, IDomainEvent
    {
        ArgumentNullException.ThrowIfNull(tag);

        lock (_gate)
        {
            ThrowIfFrozen();
            ValidateTag(tag);
            AddCore(typeof(T), tag);
        }

        return this;
    }

    /// <summary>
    /// Seed a framework default with TryAdd semantics: a consumer binding on either side of the pair
    /// wins and this returns <c>false</c>. Internal because the STORE is the only owner of the defaults
    /// (D1) — an initializer that seeded here too would give the table two owners and a count that
    /// depends on construction order.
    /// </summary>
    internal bool TryAddDefault<T>(string tag)
        where T : class, IDomainEvent
    {
        ArgumentNullException.ThrowIfNull(tag);

        lock (_gate)
        {
            // The seeds run INSIDE Freeze's critical section, and a frozen table there means a SECOND
            // construction over an options instance that was already seeded. Nothing to do, no throw:
            // two stores may share one options instance, they may not seed it twice.
            if (_frozen)
                return false;

            ValidateTag(tag);

            var type = typeof(T);
            if (_tagByType.ContainsKey(type) || _typeByTag.ContainsKey(tag))
                return false;

            _tagByType.Add(type, tag);
            _typeByTag.Add(tag, type);
            _frameworkDefaultCount++;
            return true;
        }
    }

    /// <summary>
    /// Freeze the table and hand the store its immutable copy: the defaults are seeded, the table is
    /// copied and the frozen flag is set INSIDE ONE critical section, so two threads constructing two
    /// stores over one options instance cannot double-seed, and no <see cref="Add{T}"/> can slip in
    /// between the copy and the flag. A second construction sees the same copy.
    /// </summary>
    /// <param name="frozenBy">
    /// What froze the table, named in the <see cref="Add{T}"/> refusal — the operator has to know which
    /// construction to move their bindings above.
    /// </param>
    /// <param name="seedDefaults">Applies the framework defaults, with <see cref="TryAddDefault{T}"/>.</param>
    internal EventTypeBindingsTable Freeze(string frozenBy, Action<EventTypeBindings> seedDefaults)
    {
        ArgumentNullException.ThrowIfNull(seedDefaults);

        lock (_gate)
        {
            if (_table is not null)
                return _table;

            seedDefaults(this);

            _table = new EventTypeBindingsTable(_tagByType, _typeByTag, _frameworkDefaultCount);
            _frozenBy = frozenBy;
            _frozen = true;
            return _table;
        }
    }

    /// <summary>The matrix (D1b) for a legal tag: one type, one tag; one tag, one type.</summary>
    private void AddCore(Type type, string tag)
    {
        if (_tagByType.TryGetValue(type, out var existingTag))
        {
            // Idempotent row: the same pair again is a no-op, so a start-up path that re-runs is safe.
            if (string.Equals(existingTag, tag, StringComparison.Ordinal))
                return;

            throw new InvalidOperationException(
                $"{type.FullName} is already bound to \"{existingTag}\" and cannot be bound to "
                    + $"\"{tag}\": a type has exactly ONE tag, because the tag is what a reader "
                    + "dispatches on. Remove one of the two EventTypeBindings.Add calls."
            );
        }

        if (_typeByTag.TryGetValue(tag, out var existingType))
        {
            throw new InvalidOperationException(
                $"the tag \"{tag}\" is already bound to {existingType.FullName} and cannot also name "
                    + $"{type.FullName}: a tag names exactly ONE type, or a reader could not tell the "
                    + "two events apart. Give one of them its own tag."
            );
        }

        _tagByType.Add(type, tag);
        _typeByTag.Add(tag, type);
    }

    /// <summary>
    /// The tag grammar. A tag is spliced between the quotes of the <c>$type</c> member with no escaping,
    /// so a quote, a backslash or a control character would end the string early and produce a line that
    /// is invalid JSON — and the damage would only surface on READ-BACK, possibly after the line reached
    /// an archive. Empty and blank tags name nothing at all.
    /// </summary>
    private static void ValidateTag(string tag)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            throw new ArgumentOutOfRangeException(
                nameof(tag),
                tag,
                "A tag must be a non-blank string: it IS the $type discriminator this store writes into "
                    + "every line of a bound event."
            );
        }

        foreach (var character in tag)
        {
            if (character is '"' or '\\' || char.IsControl(character))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(tag),
                    tag,
                    "A tag must not contain a double quote, a backslash or a control character: it is "
                        + "spliced into the line verbatim, so such a character would produce invalid "
                        + "JSON that only surfaces when the line is read back."
                );
            }
        }
    }

    private void ThrowIfFrozen()
    {
        if (!_frozen)
            return;

        throw new InvalidOperationException(
            $"EventTypeBindings is frozen: {_frozenBy} was constructed over these options, and a store "
                + "COPIES the bindings at construction, so a later Add would silently not apply to it. "
                + "Register every bound event before constructing the store."
        );
    }
}

/// <summary>
/// The frozen, read-only half of <see cref="EventTypeBindings"/>: what a constructed store looks at, and
/// the only structure the append and read paths touch. Both directions are copied into:
/// <see cref="System.Collections.Frozen.FrozenDictionary{TKey,TValue}"/>, so neither path pays a lock or a
/// mutable-dictionary read while running under concurrency. No reflection and no runtime code generation
/// — the dispatch itself (closed generic delegates) belongs to the paths that use it, which store their
/// own entries keyed by these types and tags.
/// </summary>
internal sealed class EventTypeBindingsTable
{
    private readonly FrozenDictionary<Type, string> _tagByType;
    private readonly FrozenDictionary<string, Type> _typeByTag;

    internal EventTypeBindingsTable(
        IReadOnlyDictionary<Type, string> tagByType,
        IReadOnlyDictionary<string, Type> typeByTag,
        int frameworkDefaultCount
    )
    {
        _tagByType = tagByType.ToFrozenDictionary();
        _typeByTag = typeByTag.ToFrozenDictionary(StringComparer.Ordinal);
        FrameworkDefaultCount = frameworkDefaultCount;
    }

    /// <summary>Bindings in this table — the store's own copy, taken at construction (D8).</summary>
    internal int Count => _tagByType.Count;

    /// <summary>Framework defaults this table carries (0..2).</summary>
    internal int FrameworkDefaultCount { get; }

    /// <summary>
    /// Every bound type. Used once, by the construction-time check that a bound type is not also
    /// <c>TEventBase</c> — one type, one registration (D5).
    /// </summary>
    internal IEnumerable<Type> BoundTypes => _tagByType.Keys;

    /// <summary>The tag a bound type is written with — the append path's lookup. A miss is not an error.</summary>
    internal bool TryGetTag(Type eventType, out string tag)
    {
        ArgumentNullException.ThrowIfNull(eventType);

        if (_tagByType.TryGetValue(eventType, out var found))
        {
            tag = found;
            return true;
        }

        tag = string.Empty;
        return false;
    }

    /// <summary>
    /// The type a tag deserializes into — the read path's lookup. A miss is not an error: the read path
    /// falls back to the existing <c>TEventBase</c> route, because "did not sniff a bound tag" must never
    /// become a new failure.
    /// </summary>
    internal bool TryGetEventType(string tag, out Type eventType)
    {
        ArgumentNullException.ThrowIfNull(tag);

        if (_typeByTag.TryGetValue(tag, out var found))
        {
            eventType = found;
            return true;
        }

        eventType = null!;
        return false;
    }
}
