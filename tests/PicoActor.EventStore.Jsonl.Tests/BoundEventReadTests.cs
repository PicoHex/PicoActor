using PicoLog.Abs;

namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// B4 — the READ path of the event-type bindings (D4/D5/D6). A line is deserialized by the route its
/// discriminator names: the FIRST member of the object is sniffed, and a tag that is bound goes through
/// that binding's closed deserializer while everything else goes through the EXISTING
/// <c>Deserialize&lt;TEventBase&gt;</c> path.
/// <para>
/// The fallback is the load-bearing half. A corpus written before bindings existed, and a base that
/// declares a custom discriminator property (<c>[PicoPolymorphic(TypeDiscriminatorPropertyName = …)]</c>),
/// must keep reading exactly as they do today — so "the sniffer did not recognise this line" is NEVER a
/// new error. Only a line that reaches the polymorphic reader and is rejected there fails, with the
/// message it has always had.
/// </para>
/// <para>
/// The sniff is a PREFIX scan over the object's first member, not a search of the whole line: a payload
/// is free to contain the text of a discriminator (Bnd13 puts a real bound tag in the second position),
/// and a whole-line search would dispatch that line to the wrong binding.
/// </para>
/// </summary>
public sealed class BoundEventReadTests
{
    /// <summary>
    /// <c>SagaCompleted(null)</c>'s line, byte for byte — the shape the production corpus already
    /// carries (2,403 + 6 records under <c>PicoActorSagaCompleted</c>), pinned by literal so the reader
    /// is held to the wire format rather than to whatever this build happens to write.
    /// </summary>
    private const string CompletedLine = "{\"$type\":\"PicoActorSagaCompleted\",\"Result\":null}";

    // ── Bnd3: the corpus tag reads, and re-writing it is byte-identical ────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd3_ExistingCorpusTag_IsReadable_AndRewritesByteIdentical(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var corpus = new TempRoot();
        var id = Guid.NewGuid();

        // A line this store did not write: no [PicoDerivedType] on ConformanceEvent declares this tag,
        // so the TEventBase route cannot read it — only the BINDING can.
        await File.WriteAllTextAsync(
            StreamPath(corpus.Root, id),
            CompletedLine + "\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(corpus.Root);
        var events = await store.LoadAsync(id);

        await Assert.That(events.Count).IsEqualTo(1);
        await Assert.That(events[0].GetType()).IsEqualTo(typeof(SagaCompleted));
        await Assert.That(events[0]).IsEqualTo(new SagaCompleted(null));

        // The rewrite: what came off the disk goes back on, and the LINE is unchanged — same tag, same
        // member order, same bytes. A read that produced a SagaCompleted but a write that spelled the
        // discriminator differently would rewrite a corpus on its first append, which is exactly what
        // this suite exists to prevent.
        using var rewritten = new TempRoot();
        var writer = new JsonlEventStore<ConformanceEvent>(rewritten.Root);
        var newId = Guid.NewGuid();
        var version = await writer.AppendAsync(newId, 0, events);
        await Assert.That(version).IsEqualTo(1UL);

        var written = await File.ReadAllBytesAsync(
            StreamPath(rewritten.Root, newId),
            cancellationToken
        );
        var expected = Encoding.UTF8.GetBytes(CompletedLine + "\n");
        await Assert.That(written.SequenceEqual(expected)).IsTrue();

        // …and the store that wrote it reads it back to the same event.
        var reloaded = await writer.LoadAsync(newId);
        await Assert.That(reloaded.Count).IsEqualTo(1);
        await Assert.That(reloaded[0]).IsEqualTo(new SagaCompleted(null));
    }

    // ── Bnd4: a tag nobody bound is loud, and names what IS bound ──────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd4_UnknownTag_IsLoud(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        await File.WriteAllTextAsync(
            StreamPath(temp.Root, id),
            "{\"$type\":\"who-knows\"}\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var error = await CaptureAsync(async () => await store.LoadAsync(id));

        // The EXISTING failure, not a new one: the line falls back to the polymorphic reader, which
        // rejects the discriminator, and the store wraps that as it always has.
        await Assert.That(error).IsTypeOf<FormatException>();

        var message = error!.Message;

        // The tag the file carries…
        await Assert.That(message.Contains("who-knows", StringComparison.Ordinal)).IsTrue();

        // …and the tags this store COULD have read, so an operator holding a file of bindings knows
        // which one is missing instead of guessing. Both framework defaults are named.
        await Assert
            .That(message.Contains(FrameworkEventTags.SagaCompleted, StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(message.Contains(FrameworkEventTags.SagaFailed, StringComparison.Ordinal))
            .IsTrue();
    }

    // ── The sniff agrees with the generated reader about what a tag IS: case does not matter ─────

    /// <summary>
    /// The generated polymorphic reader compares the discriminator VALUE case-insensitively, so this
    /// store's sniff does too — the member name and the tag alike. Writing is unaffected: the tag the
    /// write path splices is the one that was REGISTERED, verbatim, so a re-written line is canonical
    /// again.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd4b_DiscriminatorIsMatchedCaseInsensitively(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // The member NAME: `$TYPE` is the same discriminator.
        var upperName = Guid.NewGuid();
        await File.WriteAllTextAsync(
            StreamPath(temp.Root, upperName),
            "{\"$TYPE\":\"PicoActorSagaCompleted\",\"Result\":null}\n",
            cancellationToken
        );

        // The tag VALUE: a differently-cased spelling is the same tag.
        var lowerTag = Guid.NewGuid();
        await File.WriteAllTextAsync(
            StreamPath(temp.Root, lowerTag),
            "{\"$type\":\"picoactorsagacompleted\",\"Result\":null}\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        await Assert.That((await store.LoadAsync(upperName))[0]).IsEqualTo(new SagaCompleted(null));
        await Assert.That((await store.LoadAsync(lowerTag))[0]).IsEqualTo(new SagaCompleted(null));

        // The rewrite is canonical: the line comes back with the REGISTERED tag, not the spelling the
        // file happened to carry. A tag is permanent wire format in one direction only.
        using var rewritten = new TempRoot();
        var writer = new JsonlEventStore<ConformanceEvent>(rewritten.Root);
        var id = Guid.NewGuid();
        await writer.AppendAsync(id, 0, await store.LoadAsync(lowerTag));
        var lines = await File.ReadAllLinesAsync(StreamPath(rewritten.Root, id), cancellationToken);
        await Assert.That(lines[0]).IsEqualTo(CompletedLine);
    }

    // ── A bound tag whose payload is not that type's JSON gets the BINDING's message, not Wrap's ──

    /// <summary>
    /// The second half of D5's read side: a tag that IS bound but whose payload does not fit it. The
    /// failure must not tell the operator to put a base-type registration anywhere — that advice belongs
    /// to the <c>TEventBase</c> route — so it names the tag, the bound type, the file and where the tag
    /// came from instead.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd4c_PayloadThatDoesNotFitTheTag_IsWrappedByTheBinding(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        // The tag says SagaCompleted; the payload is that object and then junk, which its OWN reader
        // rejects. This is the shape a hand-edited or truncated-and-repaired file produces.
        await File.WriteAllTextAsync(
            StreamPath(temp.Root, id),
            "{\"$type\":\"PicoActorSagaCompleted\",\"Result\":null} 42\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var error = await CaptureAsync(async () => await store.LoadAsync(id));

        await Assert.That(error).IsTypeOf<FormatException>();

        var message = error!.Message;

        // The tag, the type it is bound to, and the file the line came from.
        await Assert
            .That(message.Contains(FrameworkEventTags.SagaCompleted, StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(message.Contains(typeof(SagaCompleted).FullName!, StringComparison.Ordinal))
            .IsTrue();
        await Assert.That(message.Contains(id.ToString("D"), StringComparison.Ordinal)).IsTrue();

        // WHERE the tag came from: this one is a framework default, so the operator must not go looking
        // for an Add call they never wrote.
        await Assert
            .That(message.Contains("framework default", StringComparison.OrdinalIgnoreCase))
            .IsTrue();

        // And never the wrong remedy.
        await Assert
            .That(message.Contains("[PicoDerivedType]", StringComparison.Ordinal))
            .IsFalse();

        // AND the leg is pinned, not just the source of the tag (I2). "The payload does not fit the tag"
        // has its own remedy — look at the FILE — and the write leg's has the opposite one ("nothing was
        // written: look at the type"). The two legs share one wrapper, so one wording can be lost to the
        // other in a single edit; the assertion below is the half that had no test.
        await Assert
            .That(
                message.Contains(
                    "the file holds lines this store wrote",
                    StringComparison.Ordinal
                )
            )
            .IsTrue();
        await Assert
            .That(message.Contains("could not be written", StringComparison.Ordinal))
            .IsFalse();
    }

    // ── Bnd9: routing is not serialization — a bound type with no generation point is refused ─────

    [Test, Timeout(15000)]
    public async Task Bnd9_BoundTypeWithoutGeneratedSerializer_IsRefusedOnFirstWrite(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // NoGeneratedSerializerEvent is declared in THIS test file and no call site anywhere names it
        // with a concrete type argument, so PicoJetson generated nothing for it. EventTypeBindings.Add
        // routes it and does NOT make it serializable — the failure must be loud and must name the
        // remedy, because "the append threw" alone leaves the operator with nothing to do.
        var options = new JsonlEventStoreOptions();
        options.EventTypeBindings.Add<NoGeneratedSerializerEvent>("no-serializer");

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
        var id = Guid.NewGuid();

        var error = await CaptureAsync(async () =>
        {
            _ = await store.AppendAsync(id, 0, [new NoGeneratedSerializerEvent(1)]);
        });

        await Assert.That(error).IsTypeOf<InvalidOperationException>();

        var message = error!.Message;

        // The remedy, spelled out, naming the type it applies to…
        await Assert
            .That(
                message.Contains("no generated serializer for", StringComparison.OrdinalIgnoreCase)
            )
            .IsTrue();
        await Assert
            .That(
                message.Contains(
                    typeof(NoGeneratedSerializerEvent).FullName!,
                    StringComparison.Ordinal
                )
            )
            .IsTrue();
        await Assert
            .That(message.Contains("[GenerateSerializer", StringComparison.Ordinal))
            .IsTrue();

        // …the tag the binding carries (that is the registration the operator has to find)…
        await Assert.That(message.Contains("no-serializer", StringComparison.Ordinal)).IsTrue();

        // …and never the wrong advice: [PicoDerivedType] belongs to TEventBase, which is not this type.
        await Assert
            .That(message.Contains("[PicoDerivedType]", StringComparison.Ordinal))
            .IsFalse();

        // Nothing was written and no file was created: the encode runs before the stream does.
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(0UL);
    }

    // ── Bnd11: a non-type prefix is readable by ID and invisible to TYPE enumeration (D6) ─────────

    [Test, Timeout(15000)]
    public async Task Bnd11_UnknownPrefix_IsReachableById_NotByType(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();
        var logger = new RecordingLogger();

        // The reference corpus shape: a terminal-first stream filed under a prefix that is NOT the CLR
        // type name. Nothing about a prefix is a format invariant, so this must be an ordinary
        // configured-format stream — not a foreign file, not a misconfiguration, not a warning.
        await File.WriteAllTextAsync(
            Path.Combine(temp.Root, $"unknown-{id:D}.jsonl"),
            CompletedLine + "\n",
            cancellationToken
        );

        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions
            {
                FileNameFormat = FileNameFormat.TypePrefix,
                Logger = logger,
            }
        );

        // Reachable BY ID: the map learned "unknown" from the NAME, so every read member works.
        var events = await store.LoadAsync(id);
        await Assert.That(events.Count).IsEqualTo(1);
        await Assert.That(events[0]).IsEqualTo(new SagaCompleted(null));
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(1UL);

        // Invisible to TYPE enumeration: the prefix is not the type, and the name is all a TypePrefix
        // enumeration looks at.
        var byType = await store.ListAggregateIdsAsync(nameof(SagaCompleted));
        await Assert.That(byType.Count).IsEqualTo(0);

        // NOT foreign and NOT warned about: the file parses under the configured format, so it is a
        // configured stream of another type — the ordinary case of a directory holding many types.
        await Assert.That(store.ForeignFormatFiles).IsEqualTo(0L);
        await Assert.That(store.SkippedFileNames).IsEqualTo(0L);
        await Assert.That(logger.AnyAtOrWorseThan(LogLevel.Warning)).IsFalse();
    }

    // ── Bnd12: an interface base cannot carry the registration, so it is refused at construction ───

    [Test, Timeout(15000)]
    public async Task Bnd12_InterfaceBase_IsRefusedAtConstruction(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // IDomainEvent is the interface every event implements — the tempting generic argument, and
        // the one that cannot work: [PicoDerivedType] is a type attribute on the BASE, and a type the
        // framework's own terminal events are assignable to would make them both ordinary and bound.
        var options = new JsonlEventStoreOptions();

        Exception? error = null;
        try
        {
            _ = new JsonlEventStore<IDomainEvent>(temp.Root, options);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        await Assert.That(error).IsTypeOf<InvalidOperationException>();

        var message = error!.Message;

        // The reason, not just the refusal: an operator who chose the interface has to be told it is
        // the KIND of type that is wrong, not a missing registration line.
        await Assert
            .That(message.Contains("interface", StringComparison.OrdinalIgnoreCase))
            .IsTrue();
        await Assert.That(message.Contains("[PicoDerivedType]", StringComparison.Ordinal)).IsTrue();
        await Assert
            .That(message.Contains(nameof(IDomainEvent), StringComparison.Ordinal))
            .IsTrue();

        // Refused before anything is frozen or created: the directory holds no stream.
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);

        // The SAME observable surface Bnd7 pins: the refusal came before the freeze, so the options
        // still take a binding, and the construction that threw applied none of the framework seeds.
        options.EventTypeBindings.Add<SagaFailed>("y");
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(1);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(0);
    }

    // ── Bnd13: only the FIRST member is a discriminator — anything else falls back ────────────────

    [Test, Timeout(15000)]
    public async Task Bnd13_NonFirstDiscriminator_IsTreatedAsAbsent(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        // A bound tag, in a position the polymorphic reader does not look at. The sniff is a PREFIX
        // scan, so this line is "no discriminator" — and "no discriminator" means the EXISTING route,
        // which rejects it exactly as it did before bindings existed. Two ways to get this wrong, both
        // silent: reading the tag anyway (a whole-line search finds it here) would turn a corrupt line
        // into a SagaCompleted, and treating "did not sniff" as a new error would change the failure
        // every legacy line produces.
        var line = "{\"Result\":null,\"$type\":\"PicoActorSagaCompleted\"}";
        await File.WriteAllTextAsync(StreamPath(temp.Root, id), line + "\n", cancellationToken);

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var error = await CaptureAsync(async () => await store.LoadAsync(id));

        // The pre-existing failure, unchanged: the polymorphic reader's own FormatException, wrapped by
        // the store as always.
        await Assert.That(error).IsTypeOf<FormatException>();
        await Assert.That(error!.InnerException).IsTypeOf<FormatException>();

        // NOT the binding's failure: the line never reached a binding, so a missing-serializer message
        // here would mean the sniff guessed a tag it was never given in the first position.
        await Assert
            .That(
                error.Message.Contains(
                    "no generated serializer for",
                    StringComparison.OrdinalIgnoreCase
                )
            )
            .IsFalse();

        // …and never the correct-looking answer: a read that dispatched on the second member would have
        // returned a SagaCompleted instead of throwing.
        await Assert.That(error.InnerException).IsNotNull();
        await Assert
            .That(error.Message.Contains("Cannot deserialize", StringComparison.Ordinal))
            .IsTrue();
    }

    // ── Bnd21: the sniffer's whitespace and control-byte rules, asserted by CLASSIFICATION ──────────

    /// <summary>
    /// The prefix scan tolerates JSON's insignificant whitespace and refuses everything a tag this store
    /// writes can never contain, and neither half is cosmetic: the tolerant half is what lets a line
    /// another writer INDENTED still reach its binding, and the refusing half is what keeps a control byte
    /// or an ESCAPED member name from being half-read as a discriminator.
    /// <para>
    /// Every case asserts WHICH ROUTE answered, not merely that the line did not throw. The base's
    /// polymorphic reader cannot read <c>PicoActorSagaCompleted</c> at all (Bnd3 pins that), so a returned
    /// <see cref="SagaCompleted"/> can only have come from the binding; and the fallback's wrap carries the
    /// unbound-tag clause ONLY when the sniffer found a tag (Bnd4 pins that clause), so its absence proves
    /// the sniffer DECLINED rather than sniffed-and-missed.
    /// </para>
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd21_WhitespaceAndControlBytes_AreClassifiedExactly(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        // The clause Wrap appends to a FALLBACK failure when — and only when — a tag was sniffed.
        const string UnboundTagClause = "is not one of this store's event type bindings either";

        // 0x5C, the backslash of a JSON escape (see the escaped-name case below).
        const char Backslash = (char)0x5C;

        (string Label, string Line)[] sniffed =
        [
            // `{` then a space: whitespace between tokens is insignificant JSON.
            (
                "space after the opening brace",
                "{ \"$type\":\"PicoActorSagaCompleted\",\"Result\":null}"
            ),
            ("leading space", " {\"$type\":\"PicoActorSagaCompleted\",\"Result\":null}"),
            ("leading TAB", "\t{\"$type\":\"PicoActorSagaCompleted\",\"Result\":null}"),
        ];

        foreach (var (label, line) in sniffed)
        {
            var id = Guid.NewGuid();
            await File.WriteAllTextAsync(StreamPath(temp.Root, id), line + "\n", cancellationToken);

            var events = await store.LoadAsync(id);

            // The BINDING answered: the base's reader rejects this tag, so the event below is the sniffer
            // having skipped the whitespace and found the discriminator.
            await Assert.That(events.Count).IsEqualTo(1).Because(label);
            await Assert.That(events[0]).IsEqualTo(new SagaCompleted(null)).Because(label);
        }

        (string Label, string Line)[] fallenBack =
        [
            // A control byte INSIDE the tag value. No tag this store writes carries one (ValidateTag
            // refuses every control character), so such a value is not one of ours: the line falls back
            // instead of being dispatched on HALF a tag.
            (
                "control byte in the tag value",
                "{\"$type\":\"PicoActorSagaCompleted\u0001\",\"Result\":null}"
            ),
            // An ESCAPED member name. This store never writes an escape, so a name that NEEDS one is not
            // `$type` for this reader — pinned as a FALLBACK, not as a rejection, because PicoJetson's own
            // polymorphic reader may well resolve the escaped spelling (it does: it reports the same
            // unknown discriminator), and the fallback is what keeps that answer the only one on offer.
            (
                "escaped discriminator name",
                // The backslash is spelled as a CODE POINT so no C# escape sequence can swallow it: the
                // FILE has to carry a real `\u0024` escape, and a source literal would not.
                "{" + Backslash + "u0024type\":\"PicoActorSagaCompleted\",\"Result\":null}"
            ),
            // No member at all: there is nothing to sniff.
            ("empty object", "{}"),
        ];

        foreach (var (label, line) in fallenBack)
        {
            var id = Guid.NewGuid();
            await File.WriteAllTextAsync(StreamPath(temp.Root, id), line + "\n", cancellationToken);

            var error = await CaptureAsync(async () => await store.LoadAsync(id));

            // The EXISTING failure — the polymorphic reader's own rejection, never a new one.
            await Assert.That(error).IsTypeOf<FormatException>().Because(label);

            // …and the sniffer DECLINED, which is what makes this a fallback at all.
            await Assert
                .That(error!.Message.Contains(UnboundTagClause, StringComparison.Ordinal))
                .IsFalse()
                .Because(label);
        }
    }

    /// <summary>The <c>&lt;id:D&gt;.jsonl</c> path of an IdOnly store's stream.</summary>
    private static string StreamPath(string root, Guid id) =>
        Path.Combine(root, id.ToString("D") + ".jsonl");

    /// <summary>Run <paramref name="action"/> and hand back what it threw, or <c>null</c>.</summary>
    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}

/// <summary>
/// A bound type with NO generated serializer, on purpose (Bnd9). Declared here and deliberately never
/// named by a concrete-type serialize/deserialize call: PicoJetson is call-site driven, so nothing
/// generated a serializer for it, which is the exact situation a consumer hits when it binds a type
/// whose own assembly has no generation point.
/// </summary>
public sealed record NoGeneratedSerializerEvent(int Seq) : IDomainEvent;
