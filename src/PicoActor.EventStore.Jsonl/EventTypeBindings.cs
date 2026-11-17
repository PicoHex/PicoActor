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

    // ORDINAL-IGNORE-CASE, and not for tidiness: the generated polymorphic reader matches the
    // discriminator VALUE case-insensitively, so this store's read path must too — and a tag map that
    // answered "not bound" for a differently-cased spelling of a bound tag would send a perfectly
    // readable line down the fallback path. `Bnd4b` holds the two readers against each other with a
    // differently-cased line, so the claim is measured rather than asserted. Registration follows the
    // READER: two tags that differ only in case are ONE tag, and binding
    // a second type to the other spelling is refused like any other duplicate.
    private readonly Dictionary<string, Type> _typeByTag = new(StringComparer.OrdinalIgnoreCase);

    // The append path's half of the same registration: the CLOSED generic that serializes the concrete
    // bound type (D9). Captured HERE because a generic method's type argument is the only moment this
    // package can name the type — the store serializes through a type PARAMETER, so it can never reach
    // a concrete event, and a runtime Type cannot be turned into a call site without reflection.
    private readonly Dictionary<Type, Func<IDomainEvent, byte[]>> _writeByType = new();

    // The read path's half of the same registration (D4): the CLOSED generic that deserializes the
    // concrete bound type. Keyed by TAG, because a line names a tag and knows nothing else.
    private readonly Dictionary<string, Func<byte[], IDomainEvent?>> _readByTag = new(
        StringComparer.OrdinalIgnoreCase
    );

    // WHICH tags came from this package's framework seeds rather than from a consumer's
    // Add<T> — carried so a read failure can name the registration an operator has to look at.
    private readonly HashSet<string> _defaultTags = new(StringComparer.OrdinalIgnoreCase);

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
    /// <para>
    /// Bind the type each LINE actually carries — the RUNTIME CONCRETE type an appended event has. The
    /// write path matches <see cref="object.GetType"/> exactly, so binding a base type or an abstract type
    /// has NO EFFECT: an event whose runtime type is not the bound one is written by the base's
    /// <c>[PicoDerivedType]</c> route (or refused as unbound), never by this binding. One
    /// <c>Add&lt;TConcrete&gt;</c> per concrete event, with <c>TConcrete</c> the type the event really is.
    /// </para>
    /// </summary>
    /// <typeparam name="T">
    /// The bound event type — the concrete runtime type of the lines it writes; it must be serializable in
    /// some loaded assembly.
    /// </typeparam>
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
            // The GRAMMAR first, the freeze second: a tag that could never be registered is a mistake in
            // the line the operator just typed, and it must be reported as that even when a store has
            // already frozen this table — "frozen" would send them looking for a construction when the
            // real defect is the argument in front of them.
            ValidateTag(tag);
            ThrowIfFrozen();
            AddCore(typeof(T), tag, WriterFor<T>(), ReaderFor<T>());
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
            _writeByType.Add(type, WriterFor<T>());
            _readByTag.Add(tag, ReaderFor<T>());
            _defaultTags.Add(tag);
            _frameworkDefaultCount++;
            return true;
        }
    }

    /// <summary>
    /// Freeze the table and hand the store its immutable copy: the defaults are seeded, the CANDIDATE
    /// table is built and validated, and only then are the copy and the frozen flag committed — all
    /// INSIDE ONE critical section, so two threads constructing two stores over one options instance
    /// cannot double-seed, and no <see cref="Add{T}"/> can slip in between the copy and the flag. A
    /// second construction sees the same copy.
    /// </summary>
    /// <param name="frozenBy">
    /// What froze the table, named in the <see cref="Add{T}"/> refusal — the operator has to know which
    /// construction to move their bindings above.
    /// </param>
    /// <param name="seedDefaults">Applies the framework defaults, with <see cref="TryAddDefault{T}"/>.</param>
    /// <param name="validate">
    /// Runs over the CANDIDATE table before anything is committed, and may refuse the construction by
    /// throwing. It exists so a check that needs the SEEDED table (the defaults are bindings too) lands
    /// in the same transaction: a refusal here leaves the options exactly as they were — not frozen, no
    /// default applied, no count moved — which is what makes the documented remedy ("delete the
    /// offending Add line") survivable in the SAME process. Freezing first and refusing after would hand
    /// the operator an options instance that accepts no binding and cannot be constructed over either.
    /// </param>
    internal EventTypeBindingsTable Freeze(
        string frozenBy,
        Action<EventTypeBindings> seedDefaults,
        Action<EventTypeBindingsTable>? validate = null
    )
    {
        ArgumentNullException.ThrowIfNull(seedDefaults);

        lock (_gate)
        {
            if (_table is not null)
                return _table;

            // The seeding is the only mutation this critical section makes, so capturing it is enough to
            // make the whole call all-or-nothing. The copy is a handful of dictionaries, once per store
            // constructed; being able to undo it is worth far more than that.
            var tagsBefore = new Dictionary<Type, string>(_tagByType);
            var typesBefore = new Dictionary<string, Type>(
                _typeByTag,
                StringComparer.OrdinalIgnoreCase
            );
            var writersBefore = new Dictionary<Type, Func<IDomainEvent, byte[]>>(_writeByType);
            var readersBefore = new Dictionary<string, Func<byte[], IDomainEvent?>>(
                _readByTag,
                StringComparer.OrdinalIgnoreCase
            );
            var defaultTagsBefore = new HashSet<string>(
                _defaultTags,
                StringComparer.OrdinalIgnoreCase
            );
            var frameworkDefaultCountBefore = _frameworkDefaultCount;

            seedDefaults(this);

            EventTypeBindingsTable candidate;
            try
            {
                candidate = new EventTypeBindingsTable(
                    _tagByType,
                    _typeByTag,
                    _writeByType,
                    _readByTag,
                    _defaultTags,
                    _frameworkDefaultCount
                );
                validate?.Invoke(candidate);
            }
            catch
            {
                // Rolled back BEFORE the exception leaves this lock, so no observer can see the seeds a
                // refused construction would have applied: D7 says it attests to nothing, and a count it
                // never reported is a count it must not leave behind.
                _tagByType.Clear();
                foreach (var pair in tagsBefore)
                    _tagByType.Add(pair.Key, pair.Value);
                _typeByTag.Clear();
                foreach (var pair in typesBefore)
                    _typeByTag.Add(pair.Key, pair.Value);
                _writeByType.Clear();
                foreach (var pair in writersBefore)
                    _writeByType.Add(pair.Key, pair.Value);
                _readByTag.Clear();
                foreach (var pair in readersBefore)
                    _readByTag.Add(pair.Key, pair.Value);
                _defaultTags.Clear();
                foreach (var tag in defaultTagsBefore)
                    _defaultTags.Add(tag);
                _frameworkDefaultCount = frameworkDefaultCountBefore;
                throw;
            }

            _table = candidate;
            _frozenBy = frozenBy;
            _frozen = true;
            return candidate;
        }
    }

    /// <summary>The matrix (D1b) for a legal tag: one type, one tag; one tag, one type.</summary>
    private void AddCore(
        Type type,
        string tag,
        Func<IDomainEvent, byte[]> write,
        Func<byte[], IDomainEvent?> read
    )
    {
        if (_tagByType.TryGetValue(type, out var existingTag))
        {
            // Idempotent row: the same pair again is a no-op, so a start-up path that re-runs is safe.
            // IGNORE CASE, because that is the equality the READ side uses (see _typeByTag): the
            // generated polymorphic reader accepts a differently-cased spelling of a discriminator, so
            // two spellings of one tag ARE one tag. Comparing ordinally here would refuse a second
            // registration of the tag this table already holds — a "duplicate" a reader cannot even see,
            // and a false positive on the one path a re-run start-up takes. The spelling registered
            // FIRST stays the tag that is written, so the wire format never wobbles.
            if (string.Equals(existingTag, tag, StringComparison.OrdinalIgnoreCase))
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
        _writeByType.Add(type, write);
        _readByTag.Add(tag, read);
    }

    /// <summary>
    /// The write path's delegate for <typeparamref name="T"/>: the CONCRETE serializer, resolved once at
    /// registration and never by reflection over a runtime <see cref="Type"/> (D9).
    /// <para>
    /// The call site names a TYPE PARAMETER, so it generates nothing — the same property that makes this
    /// table necessary: the serializer must already exist in some loaded assembly. This package supplies
    /// the framework's two terminal events (<see cref="FrameworkEventSerializers"/>); a consumer's own
    /// bound type must be generated in the consumer's assembly.
    /// </para>
    /// </summary>
    private static Func<IDomainEvent, byte[]> WriterFor<T>()
        where T : class, IDomainEvent =>
        domainEvent => JsonSerializer.SerializeToUtf8Bytes((T)domainEvent);

    /// <summary>
    /// The read path's delegate for <typeparamref name="T"/>: the CONCRETE deserializer, resolved once at
    /// registration. The call site names a type PARAMETER, so it generates nothing either — the reader
    /// must already exist in some loaded assembly, which is the same rule the writer obeys and the same
    /// one the read path reports when it is broken.
    /// <para>
    /// The concrete reader is the right one for a bound line: it is the serializer of the type the tag
    /// names, and it TOLERATES the leading <c>$type</c> member this store splices in front of it (the
    /// generated readers skip members they do not map), which is what makes a bound line a line of its
    /// own type and not a second wire format.
    /// </para>
    /// </summary>
    private static Func<byte[], IDomainEvent?> ReaderFor<T>()
        where T : class, IDomainEvent =>
        // AsSpan() picks the ReadOnlySpan<byte> overload explicitly: the other one takes a string, and a
        // byte[] must never be reinterpreted as text.
        data => JsonSerializer.Deserialize<T>(data.AsSpan());

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
    private readonly FrozenDictionary<Type, Func<IDomainEvent, byte[]>> _writeByType;

    // The OPENING BYTES of a bound line, per type (M2): `{"$type":"<tag>` is permanent wire format for a
    // given binding, so it is built once here and the append hot path only writes it — no interpolation
    // and no UTF-8 encoding per event.
    private readonly FrozenDictionary<Type, byte[]> _prefixByType;
    private readonly FrozenDictionary<string, Func<byte[], IDomainEvent?>> _readByTag;
    private readonly FrozenSet<string> _defaultTags;

    internal EventTypeBindingsTable(
        IReadOnlyDictionary<Type, string> tagByType,
        IReadOnlyDictionary<string, Type> typeByTag,
        IReadOnlyDictionary<Type, Func<IDomainEvent, byte[]>> writeByType,
        IReadOnlyDictionary<string, Func<byte[], IDomainEvent?>> readByTag,
        IReadOnlyCollection<string> defaultTags,
        int frameworkDefaultCount
    )
    {
        _tagByType = tagByType.ToFrozenDictionary();
        // Case-insensitive in BOTH directions lookups the read path makes: the generated polymorphic
        // reader compares the discriminator value case-insensitively, so a differently-cased spelling of
        // a bound tag is still that tag (see the registration comment).
        _typeByTag = typeByTag.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        _writeByType = writeByType.ToFrozenDictionary();
        _prefixByType = tagByType
            .ToDictionary(pair => pair.Key, pair => Encoding.UTF8.GetBytes(PrefixFor(pair.Value)))
            .ToFrozenDictionary();
        _readByTag = readByTag.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        _defaultTags = defaultTags.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
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
    /// Everything the APPEND path needs for a bound event in ONE lookup: the tag it is spliced with and
    /// the closed generic that serializes its concrete payload (D2/D9). One lookup because the two are
    /// registered together — a tag without a writer could not be written at all, so the pair is the
    /// binding, not two independent facts.
    /// </summary>
    internal bool TryGetBinding(
        Type eventType,
        out string tag,
        out Func<IDomainEvent, byte[]> write,
        out byte[] prefix
    )
    {
        ArgumentNullException.ThrowIfNull(eventType);

        if (
            _tagByType.TryGetValue(eventType, out var found)
            && _writeByType.TryGetValue(eventType, out var writer)
            && _prefixByType.TryGetValue(eventType, out var bytes)
        )
        {
            tag = found;
            write = writer;
            prefix = bytes;
            return true;
        }

        tag = string.Empty;
        write = null!;
        prefix = null!;
        return false;
    }

    /// <summary>
    /// A bound line's opening bytes, `{"$type":"&lt;tag&gt;`, built ONCE per binding (M2) — the splice is
    /// finished by the payload's own members or by the closing brace. No escaping is needed here because
    /// <see cref="EventTypeBindings.ValidateTag"/> refuses every character that would need one, which is
    /// what makes these bytes a constant of the registration rather than of the event.
    /// </summary>
    private static string PrefixFor(string tag) => $"{{\"$type\":\"{tag}\"";

    /// <summary>
    /// Everything the READ path needs for a sniffed tag in ONE lookup: the bound type and the closed
    /// generic that deserializes it (D4). One lookup for the same reason the write side takes one — the
    /// tag, the type and the two delegates are one registration.
    /// </summary>
    internal bool TryGetReadBinding(
        string tag,
        out Type eventType,
        out Func<byte[], IDomainEvent?> read
    )
    {
        ArgumentNullException.ThrowIfNull(tag);

        if (
            _typeByTag.TryGetValue(tag, out var found)
            && _readByTag.TryGetValue(tag, out var reader)
        )
        {
            eventType = found;
            read = reader;
            return true;
        }

        eventType = null!;
        read = null!;
        return false;
    }

    /// <summary>
    /// Was this tag applied by the package's own framework seeds rather than by a consumer's
    /// <see cref="EventTypeBindings.Add{T}"/>? The read path names the source in a failure, because
    /// "look at the binding" is useless advice when the operator never wrote one.
    /// </summary>
    internal bool IsFrameworkDefaultTag(string tag)
    {
        ArgumentNullException.ThrowIfNull(tag);
        return _defaultTags.Contains(tag);
    }

    /// <summary>
    /// Every tag this table can read, as a sorted, quoted list for a failure message. Sorted because a
    /// <see cref="FrozenDictionary{TKey,TValue}"/> has no enumeration order worth putting in a message an
    /// operator may match on.
    /// </summary>
    internal string DescribeTags() =>
        _tagByType.Count == 0
            ? "none registered"
            : string.Join(
                ", ",
                _tagByType
                    .Values.Distinct()
                    .Order(StringComparer.Ordinal)
                    .Select(t => "\"" + t + "\"")
            );

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
