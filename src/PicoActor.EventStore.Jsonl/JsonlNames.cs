namespace PicoActor.EventStore.Jsonl;

/// <summary>
/// What a directory entry IS, relative to the configured <see cref="FileNameFormat"/>. A scan
/// classifies every entry into exactly one of these, so a mixed directory is diagnosed entry by entry
/// instead of being silently enumerated as "no streams".
/// </summary>
internal enum StreamNameKind
{
    /// <summary>
    /// A stream of the CONFIGURED format whose name parsed: the type prefix (empty under
    /// <see cref="FileNameFormat.IdOnly"/>) and the aggregate id are known. Whether it also belongs to
    /// the queried <c>firstEventType</c> is the enumeration's decision, not the classifier's.
    /// </summary>
    ConfiguredMatch,

    /// <summary>
    /// A stream of the CONFIGURED format belonging to ANOTHER type: the name parsed, but its prefix is
    /// not the type the caller asked for. Not a diagnostic — it is counted nowhere and logged nowhere,
    /// because a directory of one format with many stream types is the NORMAL case, not a mixture.
    /// <para>
    /// Produced by the ENUMERATION side (a <see cref="ConfiguredMatch"/> whose type differs from the
    /// query), never by <see cref="JsonlNames.Classify"/>, which knows nothing about the query.
    /// </para>
    /// </summary>
    ConfiguredOtherType,

    /// <summary>
    /// A stream that is valid under the OTHER format's rule — counted in <c>ForeignFormatFiles</c> and
    /// reported once per scan. Detection is SYMMETRIC: <c>&lt;guid&gt;.jsonl</c> under
    /// <see cref="FileNameFormat.TypePrefix"/> and <c>&lt;type&gt;-&lt;guid&gt;.jsonl</c> under
    /// <see cref="FileNameFormat.IdOnly"/> are both this bucket, because pointing a store at a directory
    /// of the other layout is a whole-directory misconfiguration, never a silently empty enumeration.
    /// </summary>
    OtherFormat,

    /// <summary>
    /// The configured format's SHAPE with an unparseable id (the prefix is there, the suffix is not a
    /// <c>D</c> GUID). Counted as a skipped file name, exactly like a foreign entry.
    /// </summary>
    UnparseableId,

    /// <summary>
    /// Any other directory entry: a non-<c>.jsonl</c> name (a README, a lease, a backup) or a
    /// <c>.jsonl</c> stem that is not an id attempt at all. Counted as a skipped file name.
    /// </summary>
    Foreign,
}

/// <summary>
/// The stream-NAME grammar, in one place: <c>&lt;id:D&gt;.jsonl</c>
/// (<see cref="FileNameFormat.IdOnly"/>) and <c>&lt;type&gt;-&lt;id:D&gt;.jsonl</c>
/// (<see cref="FileNameFormat.TypePrefix"/>). The content format is identical, so the name is the only
/// thing that distinguishes a stream of one layout from a stream of the other.
/// <para>
/// Two rules are load-bearing and deliberate. The stream suffix is stripped FIRST, after an
/// <see cref="string.EndsWith(string, StringComparison)"/> guard — a short foreign name such as
/// <c>x.txt</c> is shorter than the suffix, so a bare slice would throw inside a boot-time scan. And
/// the type prefix is split at the FIRST <c>'-'</c>: a <c>D</c> GUID contains four hyphens, so a
/// <c>LastIndexOf('-')</c> split would hand back the trailing twelve hex digits as the "id".
/// </para>
/// </summary>
internal static class JsonlNames
{
    /// <summary>The stream suffix, the ONE place a stream file's extension is spelled.</summary>
    internal const string StreamSuffix = ".jsonl";

    /// <summary>The <c>D</c> format: 32 hex digits in five hyphenated groups.</summary>
    private const string GuidFormat = "D";

    /// <summary>
    /// Classify one directory entry against the CONFIGURED format. <paramref name="type"/> and
    /// <paramref name="id"/> are filled ONLY for <see cref="StreamNameKind.ConfiguredMatch"/>; the
    /// buckets exist to be counted and logged, and a name in the other format is not this store's
    /// stream even though its id is legible.
    /// </summary>
    internal static StreamNameKind Classify(
        string fileName,
        FileNameFormat configured,
        out string type,
        out Guid id
    )
    {
        ArgumentNullException.ThrowIfNull(fileName);

        type = string.Empty;
        id = Guid.Empty;

        // 1. The suffix guard comes FIRST: "x.txt" and "README.md" are shorter than ".jsonl".
        if (!fileName.EndsWith(StreamSuffix, StringComparison.Ordinal))
            return StreamNameKind.Foreign;

        var stem = fileName[..^StreamSuffix.Length];

        // 2. An empty stem (".jsonl") names nothing at all.
        if (stem.Length == 0)
            return StreamNameKind.Foreign;

        var prefixed = TrySplitTypePrefix(stem, out var prefix, out var prefixedId);
        var bare = TryParseIdOnly(stem, out var bareId);

        if (configured == FileNameFormat.TypePrefix)
        {
            if (prefixed)
            {
                type = prefix;
                id = prefixedId;
                return StreamNameKind.ConfiguredMatch;
            }

            // 3. SYMMETRY: a bare id is the IdOnly rule — the OTHER format here.
            return bare ? StreamNameKind.OtherFormat : StreamNameKind.UnparseableId;
        }

        if (bare)
        {
            id = bareId;
            return StreamNameKind.ConfiguredMatch;
        }

        // Symmetry in the other direction: the TypePrefix rule is the other format under IdOnly.
        return prefixed ? StreamNameKind.OtherFormat : StreamNameKind.UnparseableId;
    }

    /// <summary>
    /// Split <c>&lt;type&gt;-&lt;id:D&gt;</c> at its FIRST <c>'-'</c>, the only split that leaves a
    /// parseable <c>D</c> GUID: the prefix must be non-empty and the remainder must be the GUID.
    /// </summary>
    internal static bool TrySplitTypePrefix(string stem, out string type, out Guid id)
    {
        ArgumentNullException.ThrowIfNull(stem);

        type = string.Empty;
        id = Guid.Empty;

        var separator = stem.IndexOf('-', StringComparison.Ordinal);
        if (separator <= 0)
            return false;

        if (!Guid.TryParseExact(stem.AsSpan(separator + 1), GuidFormat, out id))
            return false;

        type = stem[..separator];
        return true;
    }

    /// <summary>
    /// Parse <c>&lt;id:D&gt;</c> — the whole stem IS the id, so nothing is split and nothing is
    /// searched for.
    /// </summary>
    internal static bool TryParseIdOnly(string stem, out Guid id)
    {
        ArgumentNullException.ThrowIfNull(stem);

        return Guid.TryParseExact(stem, GuidFormat, out id);
    }

    /// <summary>
    /// Parse a file name under the OTHER format's rule — the layout this store does NOT read — and hand
    /// back the id it carries. This is what the duplicate guard needs: under
    /// <see cref="FileNameFormat.IdOnly"/> the other layout's prefix is not derivable from an id, so a
    /// directory pass is the only way to find the same aggregate there.
    /// </summary>
    internal static bool TryParseOtherFormat(
        string fileName,
        FileNameFormat configured,
        out Guid id
    )
    {
        ArgumentNullException.ThrowIfNull(fileName);

        id = Guid.Empty;

        // Same guard ORDER as Classify: the suffix is stripped only after it is known to be there, so a
        // short foreign name never reaches the slice.
        if (!fileName.EndsWith(StreamSuffix, StringComparison.Ordinal))
            return false;

        var stem = fileName[..^StreamSuffix.Length];
        if (stem.Length == 0)
            return false;

        return configured == FileNameFormat.TypePrefix
            ? TryParseIdOnly(stem, out id)
            : TrySplitTypePrefix(stem, out _, out id);
    }

    /// <summary>The layout this store does NOT read — the one a mixed directory is reported against.</summary>
    internal static FileNameFormat Opposite(FileNameFormat format) =>
        format == FileNameFormat.TypePrefix ? FileNameFormat.IdOnly : FileNameFormat.TypePrefix;

    /// <summary>
    /// A layout's name, spelled for a human. A diagnostic that only says "the other format" leaves the
    /// operator unable to decide between migrating the directory and flipping the option.
    /// </summary>
    internal static string Describe(FileNameFormat format) =>
        format == FileNameFormat.TypePrefix
            ? "TypePrefix (<type>-<id>.jsonl)"
            : "IdOnly (<id>.jsonl)";
}
