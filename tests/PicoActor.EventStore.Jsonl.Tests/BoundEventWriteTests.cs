using System.Buffers;
using PicoJetson;
using PicoSerDe.Core;

namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// B3 — the WRITE path of the event-type bindings (D2/D3). A store's accepted set is
/// <c>TEventBase</c> <b>∪</b> every bound type, and a batch that mixes the two is written exactly like
/// any other batch: ONE buffer, one <c>WriteAsync</c>, one <c>FlushAsync</c>.
/// <para>
/// What these tests pin is the WIRE FORMAT, by literal. A bound event is not serialized a second way —
/// its concrete payload keeps every byte it would have had, and the discriminator is spliced in front of
/// it (<c>'{"$type":"' + tag + '"' + payload[1..]</c>: no comma, and the payload's opening brace
/// dropped). The recipe matters because the wrong one — a comma, or the payload written whole — produces
/// <c>{"$type":"t",{"Result":null}</c>, which is invalid JSON that would only surface on read-back,
/// possibly after the line reached an archive.
/// </para>
/// <para>
/// The store has no write/flush counter, so "one buffer, one write" is observed through what it produces:
/// the version, the exact bytes on disk, and a clean log (no torn-tail Warning) — nothing else is
/// claimed here.
/// </para>
/// </summary>
public sealed class BoundEventWriteTests
{
    /// <summary><c>SagaCompleted(null)</c>'s line, byte for byte (D2's recipe spelled out).</summary>
    private const string NullResultLine = "{\"$type\":\"PicoActorSagaCompleted\",\"Result\":null}";

    /// <summary><c>SSnapshotAdded(7)</c>'s line — unchanged by this task, and asserted unchanged.</summary>
    private const string BusinessLine = "{\"$type\":\"ss-added\",\"Delta\":7}";

    // ── Bnd1: a mixed batch is one append, and both lines land in order ─────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd1_MixedBatch_RoundTrips(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var store = new JsonlEventStore<ConformanceEvent>(
            temp.Root,
            new JsonlEventStoreOptions { Logger = logger }
        );
        var id = Guid.NewGuid();

        // The saga shape: business events and the framework's terminal completion arrive in the SAME
        // flush, so they are one call — and must be one write.
        var version = await store.AppendAsync(
            id,
            0,
            [new SSnapshotAdded(7), new SagaCompleted(null)]
        );

        // The observable triple the spec names — version, lines, clean log.
        await Assert.That(version).IsEqualTo(2UL);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(2UL);

        var path = StreamPath(temp.Root, id);
        var lines = await File.ReadAllLinesAsync(path, cancellationToken);
        await Assert.That(lines.Length).IsEqualTo(2);

        // Order: the business event first, the completion second. And the bytes themselves: the whole
        // file, framing included, so a mis-splice cannot hide behind a line-oriented read.
        await Assert.That(lines[0]).IsEqualTo(BusinessLine);
        await Assert.That(lines[1]).IsEqualTo(NullResultLine);

        var expected = Encoding.UTF8.GetBytes(BusinessLine + "\n" + NullResultLine + "\n");
        var written = await File.ReadAllBytesAsync(path, cancellationToken);
        await Assert.That(written.SequenceEqual(expected)).IsTrue();

        // Nothing was torn or repaired: the batch reached the disk as one terminated payload.
        await Assert.That(store.TornTailLines).IsEqualTo(0L);
        await Assert.That(logger.Contains("torn")).IsFalse();

        // … and the file is READ BACK, because every assertion above only ever looked at bytes this
        // test already knew how to spell: a write the reader could not follow would have passed all of
        // them. A mixed batch is the ONE shape that exercises both routes of one file, and it must come
        // back in append order — the business event through the base's polymorphic reader, the
        // completion through the framework tag's binding.
        var loaded = await store.LoadAsync(id);
        await Assert.That(loaded.Count).IsEqualTo(2);
        await Assert.That(loaded[0].GetType()).IsEqualTo(typeof(SSnapshotAdded));
        await Assert.That(loaded[0]).IsEqualTo(new SSnapshotAdded(7));
        await Assert.That(loaded[1].GetType()).IsEqualTo(typeof(SagaCompleted));
        await Assert.That(loaded[1]).IsEqualTo(new SagaCompleted(null));
    }

    // ── Bnd2: a stream's prefix is still its FIRST event's CLR type name (D6) ──────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd2_SagaOnlyStream_PrefixIsTheTypeName(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var id = Guid.NewGuid();

        // A saga stream that carries ONLY the terminal completion: the naming rule is unchanged
        // (events[0].GetType().Name), so the file is named after the COMPLETION — never after the
        // store's base type, and never after the tag.
        var writer = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await writer.AppendAsync(id, 0, [new SagaCompleted(null)]);

        var name = $"{nameof(SagaCompleted)}-{id:D}.jsonl";
        await Assert.That(File.Exists(Path.Combine(temp.Root, name))).IsTrue();

        // A COLD store learns its map from the NAMES, so enumerating the type is answered with zero
        // streams opened — the counter is the claim, and the ids are asserted so an empty set cannot
        // satisfy it.
        var reader = new JsonlEventStore<ConformanceEvent>(temp.Root, TypePrefix());
        await Assert.That(reader.FilesOpened).IsEqualTo(0L);

        var ids = await reader.ListAggregateIdsAsync(nameof(SagaCompleted));
        await Assert.That(ids.Count).IsEqualTo(1);
        await Assert.That(ids).Contains(id);
        await Assert.That(reader.FilesOpened).IsEqualTo(0L);
    }

    // ── Bnd5: neither TEventBase nor bound — the old refusal, with the new remedy appended (D3/D5) ─

    [Test, Timeout(15000)]
    public async Task Bnd5_UnboundEvent_IsRefused_WithTheRemedy(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();

        // ForeignEvent implements IDomainEvent only: it is neither a ConformanceEvent nor a bound type,
        // so the store has no route for it in either direction.
        var error = await CaptureAsync(async () =>
        {
            _ = await store.AppendAsync(id, 0, [new SSnapshotAdded(1), new ForeignEvent(2)]);
        });

        await Assert.That(error is ArgumentException).IsTrue();

        // The EXISTING wording survives verbatim: an operator (or a runbook) that already matches it
        // must not be orphaned, and "[PicoDerivedType] on the base" is still the right first remedy for
        // an event the base could have covered.
        var message = error!.Message;
        await Assert
            .That(
                message.Contains(
                    $"which is not a {typeof(ConformanceEvent).FullName}",
                    StringComparison.Ordinal
                )
            )
            .IsTrue();
        await Assert.That(message.Contains("[PicoDerivedType]", StringComparison.Ordinal)).IsTrue();

        // The index: in a mixed batch the operator needs to know WHICH element is the offender.
        await Assert.That(message.Contains("index 1", StringComparison.Ordinal)).IsTrue();

        // …and the SECOND remedy is appended, so an event that must not derive from the base is not a
        // dead end.
        await Assert
            .That(
                message.Contains(
                    nameof(JsonlEventStoreOptions.EventTypeBindings),
                    StringComparison.Ordinal
                )
            )
            .IsTrue();

        // The encode runs before the file is created, so a refused append leaves no file behind — and
        // that is still true when the refuser is the SECOND element of a batch whose first one is fine.
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(0UL);
    }

    // ── Bnd10: the naming format changes a NAME, never a line ──────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd10_IdOnly_And_TypePrefix_Agree(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var idOnlyRoot = new TempRoot();
        using var typePrefixRoot = new TempRoot();
        var id = Guid.NewGuid();

        // Two independent directories: one id must never exist in both layouts (that is the
        // mixed-directory condition the store diagnoses), so each format owns its own root.
        var idOnly = new JsonlEventStore<ConformanceEvent>(idOnlyRoot.Root);
        var typePrefix = new JsonlEventStore<ConformanceEvent>(typePrefixRoot.Root, TypePrefix());

        IDomainEvent[] batch = [new SSnapshotAdded(7), new SagaCompleted(null)];

        var idOnlyVersion = await idOnly.AppendAsync(id, 0, batch);
        var prefixVersion = await typePrefix.AppendAsync(id, 0, batch);

        await Assert.That(idOnlyVersion).IsEqualTo(2UL);
        await Assert.That(prefixVersion).IsEqualTo(idOnlyVersion);
        await Assert.That(await idOnly.GetVersionAsync(id)).IsEqualTo(2UL);
        await Assert.That(await typePrefix.GetVersionAsync(id)).IsEqualTo(2UL);

        // Byte-identical streams: the format decides the FILE NAME and nothing else.
        var idOnlyBytes = await File.ReadAllBytesAsync(
            StreamPath(idOnlyRoot.Root, id),
            cancellationToken
        );
        var prefixBytes = await File.ReadAllBytesAsync(
            Path.Combine(typePrefixRoot.Root, $"{nameof(SSnapshotAdded)}-{id:D}.jsonl"),
            cancellationToken
        );
        await Assert.That(prefixBytes.SequenceEqual(idOnlyBytes)).IsTrue();
        await Assert
            .That(
                prefixBytes.SequenceEqual(
                    Encoding.UTF8.GetBytes(BusinessLine + "\n" + NullResultLine + "\n")
                )
            )
            .IsTrue();
    }

    // ── Bnd15: the object? member reaches the wire — and says so when it cannot ────────────────────

    /// <summary>
    /// <see cref="SagaCompleted.Result"/> is <c>object?</c>, and PicoSerDe 2026.14 writes such a member
    /// as whatever the RUNTIME value is (its "any-value" chain), so the store no longer has to refuse
    /// everything but <c>null</c>. Three facts are pinned here, because each one is a promise an operator
    /// reads a corpus by:
    /// <list type="number">
    /// <item><c>null</c> — the shape the framework itself produces, since every <c>MarkComplete</c> call
    /// site is parameterless — is the literal D2 line, byte for byte, and reads back as
    /// <c>SagaCompleted(null)</c>.</item>
    /// <item>A <see cref="string"/> round-trips AS a string: the member's CLR type is <c>object</c>, so
    /// "what comes back" is a question about the VALUE, and the answer is the value that went out.</item>
    /// <item>An integer is the documented LOSS: it goes out as <c>42</c> and comes back as a
    /// <see cref="long"/>, never as the <c>int</c>-holding box that was handed in. A consumer that
    /// pattern-matches <c>is int</c> on a rebuilt completion is reading a wire with no integer width on
    /// it — this test exists so that is a pinned fact rather than a surprise.</item>
    /// </list>
    /// The fourth case is the one the store owes an operator: a runtime type with NO JSON form
    /// (<c>new object()</c>, an arbitrary POCO) must fail LOUDLY, name the event that could not be
    /// written, and leave nothing behind — no file, no version. A silent <c>null</c>, a half-written line
    /// or an unreadable one would all be worse than the refusal.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd15_ResultRoundTrip_UnrepresentableValueFailsLoudly(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);

        // ── 1. null: D2's line, pinned by literal ─────────────────────────────────────────────────
        var nullId = Guid.NewGuid();
        await store.AppendAsync(nullId, 0, [new SagaCompleted(null)]);

        var nullLines = await File.ReadAllLinesAsync(
            StreamPath(temp.Root, nullId),
            cancellationToken
        );
        await Assert.That(nullLines.Length).IsEqualTo(1);

        // If the discriminator were spliced by keeping the payload whole, the line would read
        // `{"$type":"PicoActorSagaCompleted",{"Result":null}` — the payload's own brace kept AND a comma
        // in front of it: invalid JSON, invisible to this write, fatal on read-back. The literal below is
        // the byte string the production corpus already carries (Bnd3 pins it from the read side), which
        // is why the payload's opening brace must be DROPPED.
        await Assert.That(nullLines[0]).IsEqualTo(NullResultLine);

        // Round trip through a CONCRETE deserializer too: the extra leading `$type` member is tolerated
        // (D2's invariant), so the line this store writes is a SagaCompleted line and not merely a
        // well-formed one. Reached through a METHOD GROUP on purpose — an invocation here would make the
        // test assembly generate the serializer itself and blind Bnd0's tripwire.
        DeserializeSagaCompleted deserialize = JsonSerializer.Deserialize<SagaCompleted>;
        var nullResult = deserialize(Encoding.UTF8.GetBytes(nullLines[0]), null);
        await Assert.That(nullResult).IsEqualTo(new SagaCompleted(null));
        await Assert.That(nullResult!.Result).IsNull();

        var nullReloaded = await store.LoadAsync(nullId);
        await Assert.That(nullReloaded.Count).IsEqualTo(1);
        await Assert.That(nullReloaded[0].GetType()).IsEqualTo(typeof(SagaCompleted));
        await Assert.That(nullReloaded[0]).IsEqualTo(new SagaCompleted(null));

        // ── 2. a string round-trips AS a string ──────────────────────────────────────────────────
        var stringId = Guid.NewGuid();
        await store.AppendAsync(stringId, 0, [new SagaCompleted("boom")]);

        var stringLines = await File.ReadAllLinesAsync(
            StreamPath(temp.Root, stringId),
            cancellationToken
        );
        await Assert.That(stringLines.Length).IsEqualTo(1);
        await Assert
            .That(stringLines[0])
            .IsEqualTo("{\"$type\":\"PicoActorSagaCompleted\",\"Result\":\"boom\"}");

        var stringReloaded = await store.LoadAsync(stringId);
        await Assert.That(stringReloaded.Count).IsEqualTo(1);
        var stringCompleted = stringReloaded[0] as SagaCompleted;
        await Assert.That(stringCompleted).IsNotNull();

        // The VALUE, not the box: `object?` means the rebuilt member is whatever the wire said, and the
        // wire said a JSON string.
        await Assert.That(stringCompleted!.Result is string).IsTrue();
        await Assert.That(stringCompleted.Result).IsEqualTo("boom");
        await Assert.That(stringCompleted).IsEqualTo(new SagaCompleted("boom"));

        // ── 3. an integer goes out as 42 and comes back as a LONG ────────────────────────────────
        var numberId = Guid.NewGuid();
        await store.AppendAsync(numberId, 0, [new SagaCompleted(42L)]);

        var numberLines = await File.ReadAllLinesAsync(
            StreamPath(temp.Root, numberId),
            cancellationToken
        );
        await Assert.That(numberLines.Length).IsEqualTo(1);
        // One JSON number, exactly as the value was: no quotes, no decimal point.
        await Assert
            .That(numberLines[0].Contains("\"Result\":42", StringComparison.Ordinal))
            .IsTrue();

        var numberReloaded = await store.LoadAsync(numberId);
        await Assert.That(numberReloaded.Count).IsEqualTo(1);
        var numberCompleted = numberReloaded[0] as SagaCompleted;
        await Assert.That(numberCompleted).IsNotNull();
        await Assert.That(numberCompleted!.Result is long).IsTrue();
        await Assert.That(numberCompleted.Result).IsEqualTo(42L);
        // The promotion, spelled out: it is NOT the int that went in, and a consumer matching on `int`
        // over a rebuilt corpus finds nothing.
        await Assert.That(numberCompleted.Result is int).IsFalse();

        // ── 4. a runtime type with no JSON form fails LOUDLY, and writes nothing ─────────────────
        var refusedId = Guid.NewGuid();
        var error = await CaptureAsync(async () =>
        {
            _ = await store.AppendAsync(refusedId, 0, [new SagaCompleted(new object())]);
        });

        // "Loud" is pinned, not merely observed: the failure is one of the two documented shapes — the
        // serializer's FormatException for a value with no JSON form, an InvalidOperationException for a
        // missing serializer — and it NAMES the event. A bare `error is not null` would accept any
        // exception at all, including a NullReferenceException from this store, which is exactly the
        // regression this assertion is here to refuse.
        await Assert.That(error is FormatException or InvalidOperationException).IsTrue();
        await Assert
            .That(error!.Message.Contains(nameof(SagaCompleted), StringComparison.Ordinal))
            .IsTrue();

        // WHOSE failure is it (I2): the WRITE leg's, and the remedy has to say so. Both legs share one
        // wrapper, and the READ leg's advice — "check that the file holds lines this store wrote" — was
        // being handed to a caller who is MID-APPEND: there is no line and there is no file, so that
        // sentence sends the operator looking for something this failure never created.
        await Assert
            .That(
                error.Message.Contains(
                    "the file holds lines this store wrote",
                    StringComparison.Ordinal
                )
            )
            .IsFalse();
        await Assert
            .That(error.Message.Contains("could not be written", StringComparison.Ordinal))
            .IsTrue();

        // Nothing was written: no line, no file, no version.
        await Assert.That(File.Exists(StreamPath(temp.Root, refusedId))).IsFalse();
        await Assert.That(await store.GetVersionAsync(refusedId)).IsEqualTo(0UL);
    }

    // ── The zero-member payload: the splice must not leave a dangling separator (D2) ───────────────

    /// <summary>
    /// D2 spells two splice outcomes, and the second one is the reason the separator is CONDITIONAL: a
    /// payload with no members is <c>{}</c>, whose tail after the dropped brace is just <c>}</c>, so
    /// `{"$type":"<tag>"` must NOT be joined with a comma — `{"$type":"t",}` is invalid JSON. A consumer
    /// that binds a memberless "milestone" event hits exactly this, so the branch is pinned rather than
    /// left to the reasoning in the write path.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd1b_ZeroMemberPayload_WritesTheTagAlone(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // A consumer-owned bound type: its serializer has to be generated in THIS assembly (D2b), which
        // is what the literal call below does — the documented consumer route.
        _ = JsonSerializer.SerializeToUtf8Bytes(new MilestoneReached());

        var options = new JsonlEventStoreOptions();
        options.EventTypeBindings.Add<MilestoneReached>("milestone");
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
        var id = Guid.NewGuid();

        await store.AppendAsync(id, 0, [new MilestoneReached()]);

        var lines = await File.ReadAllLinesAsync(StreamPath(temp.Root, id), cancellationToken);
        await Assert.That(lines.Length).IsEqualTo(1);
        await Assert.That(lines[0]).IsEqualTo("{\"$type\":\"milestone\"}");

        // …and it READS back. The writing half alone would let a splice that produces a line this store
        // cannot parse pass: a memberless payload is the one shape where the tag is the WHOLE object, so
        // this is the case where "the reader tolerates the leading $type member" has to hold with no
        // member behind it to prove the payload survived.
        var reloaded = await store.LoadAsync(id);
        await Assert.That(reloaded.Count).IsEqualTo(1);
        await Assert.That(reloaded[0].GetType()).IsEqualTo(typeof(MilestoneReached));
        await Assert.That(reloaded[0]).IsEqualTo(new MilestoneReached());
    }

    // ── Bnd18: a bound payload that is not a JSON object is REFUSED, not spliced (D2) ──────────────

    /// <summary>
    /// The splice DROPS the payload's opening brace and assumes a member list behind it, so a payload that
    /// is not a JSON object would be framed into a line that is not JSON at all — invisible at write time
    /// and fatal on read-back, possibly after the line reached an archive. That guard is REACHABLE from a
    /// consumer: <see cref="JsonSerializer.RegisterCustom{T}"/> replaces the top-level writer for any type
    /// it is given, so ONE public call is all it takes to make a bound type write <c>[1,2]</c> — and the
    /// store must refuse the append rather than splice it.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd18_NonObjectBoundPayload_IsRefusedAndWritesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // The consumer's own serializer: the one public hook that can produce a non-object payload, since
        // a generated writer for a record always writes an object.
        JsonSerializer.RegisterCustom(new ArrayPayloadSerializer(), new ArrayPayloadDeserializer());

        var options = new JsonlEventStoreOptions();
        options.EventTypeBindings.Add<ArrayPayloadEvent>("arr");
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
        var id = Guid.NewGuid();

        var error = await CaptureAsync(async () =>
        {
            _ = await store.AppendAsync(id, 0, [new ArrayPayloadEvent()]);
        });

        // The refusal is this store's own FormatException — raised AFTER the payload arrived and BEFORE a
        // byte reached the buffer — not the serializer's.
        await Assert.That(error).IsTypeOf<FormatException>();

        var message = error!.Message;

        // The tag, the bound type and WHAT arrived (the payload's first bytes): the three facts an operator
        // holding a type that writes the wrong shape needs to identify it.
        await Assert.That(message.Contains("\"arr\"", StringComparison.Ordinal)).IsTrue();
        await Assert
            .That(message.Contains(typeof(ArrayPayloadEvent).FullName!, StringComparison.Ordinal))
            .IsTrue();
        await Assert.That(message.Contains("[1,2]", StringComparison.Ordinal)).IsTrue();

        // Nothing was written: no line, no file, no version.
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);
        await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(0UL);
    }

    // ── Bnd18b: "memberless" is a question about the JSON, not about the payload's LENGTH (D2) ─────

    /// <summary>
    /// The separator is emitted only when there is a MEMBER to separate, and the payload the splice
    /// examines is whatever the type's own writer produced: <c>{}</c> for a memberless record, and
    /// <c>{ }</c>, <c>{  }</c> or <c>{ }</c> followed by a newline for the SAME object written by a
    /// consumer's <see cref="JsonSerializer.RegisterCustom{T}"/> serializer. A <c>Length &gt; 2</c> test
    /// calls them "has members" and writes <c>{"$type":"t", }</c> — a trailing comma, invalid JSON,
    /// invisible at write time and fatal on read-back. Neither a LENGTH nor a "the closing brace sits at
    /// <c>Length - 1</c>" POSITION answers the question: the last row below is the one a positional test
    /// got wrong, splicing a separator in front of a brace that had merely arrived early. Every row must
    /// write the tag ALONE.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd18b_MemberlessPayload_IsAlwaysWrittenWithoutASeparator(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        // Every row but `{}` needs a writer that emits the payload; a generated one writes `{}` and
        // nothing else, so those are the consumer hook again.
        JsonSerializer.RegisterCustom(
            new ShapedPayloadSerializer(),
            new ShapedPayloadDeserializer()
        );
        // The `{}` row is MilestoneReached's own generated writer (Bnd1b pins that exact path).
        _ = JsonSerializer.SerializeToUtf8Bytes(new MilestoneReached());

        (string Label, IDomainEvent Event, string Expected)[] cases =
        [
            (
                "generated memberless payload ({}):",
                new MilestoneReached(),
                "{\"$type\":\"memberless\"}"
            ),
            (
                "whitespace-only memberless payload ({ }):",
                new ShapedPayloadEvent("{ }"),
                "{\"$type\":\"shaped\"}"
            ),
            (
                "wider whitespace-only memberless payload ({  }):",
                new ShapedPayloadEvent("{  }"),
                "{\"$type\":\"shaped\"}"
            ),
            (
                "memberless payload with a trailing newline ({}\n):",
                new ShapedPayloadEvent("{}\n"),
                "{\"$type\":\"shaped\"}"
            ),
            (
                "memberless payload with a space and a trailing newline ({ }\n):",
                new ShapedPayloadEvent("{ }\n"),
                "{\"$type\":\"shaped\"}"
            ),
        ];

        foreach (var (label, boundEvent, expected) in cases)
        {
            using var temp = new TempRoot();
            var options = new JsonlEventStoreOptions();
            // BOTH bindings on every store, so the loop differs only in the event it appends.
            options.EventTypeBindings.Add<MilestoneReached>("memberless");
            options.EventTypeBindings.Add<ShapedPayloadEvent>("shaped");
            var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
            var id = Guid.NewGuid();

            await store.AppendAsync(id, 0, [boundEvent]);

            var lines = await File.ReadAllLinesAsync(StreamPath(temp.Root, id), cancellationToken);
            await Assert.That(lines.Length).IsEqualTo(1).Because(label);
            await Assert.That(lines[0]).IsEqualTo(expected).Because(label);

            // …and it READS BACK as the bound type: what follows the tag must still be an object this
            // store's own sniff routes, not a separator with nothing behind it.
            var reloaded = await store.LoadAsync(id);
            await Assert.That(reloaded.Count).IsEqualTo(1).Because(label);
            await Assert.That(reloaded[0].GetType()).IsEqualTo(boundEvent.GetType()).Because(label);
        }
    }

    // ── Bnd18c: the payload's OBJECT may start after whitespace too (D2) ───────────────────────────

    /// <summary>
    /// The guard's question is "is the payload a JSON OBJECT", and whitespace in front of the brace does
    /// not stop it from being one: a writer that indents, or that emits a leading newline, produced a
    /// perfectly valid object. A <c>payload[0] != '{'</c> test refuses it with the wording that belongs
    /// to <c>[1,2]</c> and <c>42</c> — the operator is told their type writes a non-object when it writes
    /// one — while the splice drops a BRACE, not byte 0. The start check therefore skips the same
    /// whitespace the memberless check does, and the line is framed as if the payload had begun at its
    /// brace.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd18c_PayloadWithLeadingWhitespace_IsStillAnObject(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        JsonSerializer.RegisterCustom(
            new ShapedPayloadSerializer(),
            new ShapedPayloadDeserializer()
        );

        // One of each whitespace byte JSON allows, then a real object. A generated writer never emits
        // this; the consumer hook does, and the refusal must not call it a non-object.
        const string payload = " \t\r\n{\"Result\":null}";

        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions();
        options.EventTypeBindings.Add<ShapedPayloadEvent>("shaped");
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
        var id = Guid.NewGuid();

        await store.AppendAsync(id, 0, [new ShapedPayloadEvent(payload)]);

        var lines = await File.ReadAllLinesAsync(StreamPath(temp.Root, id), cancellationToken);
        await Assert.That(lines.Length).IsEqualTo(1);
        // The blanks in front of the brace leave WITH the brace — they frame nothing — and the payload's
        // own member list follows the tag unchanged.
        await Assert.That(lines[0]).IsEqualTo("{\"$type\":\"shaped\",\"Result\":null}");

        var reloaded = await store.LoadAsync(id);
        await Assert.That(reloaded.Count).IsEqualTo(1);
        await Assert.That(reloaded[0].GetType()).IsEqualTo(typeof(ShapedPayloadEvent));
    }

    // ── Bnd18d: nothing behind the brace is not an object either (D2) ───────────────────────────────

    /// <summary>
    /// Every memberless row above has a <c>}</c> to find. <c>{</c> followed by nothing but blanks has no
    /// closing brace and no member, so it is the ONE shape where "skip the blanks behind the brace" runs off
    /// the end of the payload: a check that stopped at <c>Length - 1</c>, or that only asked "is the first
    /// byte a <c>{</c>", would splice it into <c>{"$type":"shaped",   </c> — a separator with nothing behind
    /// it, invalid JSON, invisible at write time and fatal on read-back. The refusal is the SAME one a
    /// <c>[1,2]</c> payload gets, because the payload is equally not an object.
    /// <para>
    /// The bytes come from the consumer hook (<see cref="JsonSerializer.RegisterCustom{T}"/>), which is the
    /// only way to reach this shape: a generated writer always closes its brace.
    /// </para>
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd18d_NothingBehindTheBrace_IsRefusedAndWritesNothing(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        JsonSerializer.RegisterCustom(
            new ShapedPayloadSerializer(),
            new ShapedPayloadDeserializer()
        );

        // `{` alone, then `{` with blanks only — the two spellings of the same truncated object. The
        // second is the one a "stop at Length - 1" test would have let through.
        string[] payloads = ["{", "{   "];

        foreach (var payload in payloads)
        {
            using var temp = new TempRoot();
            var options = new JsonlEventStoreOptions();
            options.EventTypeBindings.Add<ShapedPayloadEvent>("shaped");
            var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
            var id = Guid.NewGuid();

            var error = await CaptureAsync(async () =>
            {
                _ = await store.AppendAsync(id, 0, [new ShapedPayloadEvent(payload)]);
            });

            // This store's own FormatException, not an index failure from inside the splice and not a
            // malformed line on disk.
            await Assert.That(error).IsTypeOf<FormatException>().Because(payload);

            var message = error!.Message;

            await Assert
                .That(message.Contains("\"shaped\"", StringComparison.Ordinal))
                .IsTrue()
                .Because(payload);
            await Assert
                .That(
                    message.Contains(typeof(ShapedPayloadEvent).FullName!, StringComparison.Ordinal)
                )
                .IsTrue()
                .Because(payload);
            await Assert
                .That(message.Contains("must be a JSON object", StringComparison.Ordinal))
                .IsTrue()
                .Because(payload);

            // Nothing was written: no line, no file, no version.
            await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0).Because(payload);
            await Assert.That(await store.GetVersionAsync(id)).IsEqualTo(0UL).Because(payload);
        }
    }

    /// <summary>
    /// <c>Deserialize&lt;SagaCompleted&gt;</c>'s shape as a delegate — a custom delegate because a
    /// generic <see cref="Func{T,TResult}"/> cannot carry a <see cref="ReadOnlySpan{T}"/> parameter.
    /// </summary>
    private delegate SagaCompleted? DeserializeSagaCompleted(
        ReadOnlySpan<byte> data,
        JsonOptions? options = null
    );

    private static string StreamPath(string root, Guid id) =>
        Path.Combine(root, id.ToString("D") + ".jsonl");

    private static JsonlEventStoreOptions TypePrefix() =>
        new() { FileNameFormat = FileNameFormat.TypePrefix };

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
/// A memberless event a CONSUMER might bind (D2's zero-member payload case). Declared here on purpose:
/// it is not a store type, and the test above is the consumer of its serializer.
/// </summary>
public sealed record MilestoneReached : IDomainEvent;

/// <summary>
/// A bound type whose own serializer writes a JSON ARRAY (Bnd18). A consumer reaches this shape with one
/// public call — <c>JsonSerializer.RegisterCustom</c> — which is why the store's payload guard is a
/// reachable refusal and not an unreachable invariant.
/// </summary>
public sealed record ArrayPayloadEvent : IDomainEvent;

/// <summary>
/// A bound type whose own writer is the TEST's (Bnd18b/Bnd18c):
/// <see cref="JsonSerializer.RegisterCustom{T}"/> replaces the top-level writer for the type it is given,
/// so <see cref="Payload"/> is never serialized — those bytes ARE the payload. It is the one public hook
/// into the payload's SHAPE, and the shapes a generated writer never produces (<c>{ }</c>, <c>{  }</c>,
/// <c>{}</c> plus a newline, an object behind leading blanks) are exactly the ones the splice must
/// survive: a downstream serializer is free to pretty-print.
/// </summary>
public sealed record ShapedPayloadEvent(string Payload) : IDomainEvent;

/// <summary>Writes <c>[1,2]</c> for every <see cref="ArrayPayloadEvent"/>.</summary>
internal sealed class ArrayPayloadSerializer : ISerializer<ArrayPayloadEvent>
{
    public void Serialize(IBufferWriter<byte> writer, ArrayPayloadEvent value) =>
        writer.Write("[1,2]"u8);
}

/// <summary>Reads nothing: the write leg is the subject, and a refused append never reaches a read.</summary>
internal sealed class ArrayPayloadDeserializer : IDeserializer<ArrayPayloadEvent>
{
    public ArrayPayloadEvent Deserialize(ReadOnlySpan<byte> data) => new();
}

/// <summary>Writes the test's own bytes, verbatim, for every <see cref="ShapedPayloadEvent"/>.</summary>
internal sealed class ShapedPayloadSerializer : ISerializer<ShapedPayloadEvent>
{
    public void Serialize(IBufferWriter<byte> writer, ShapedPayloadEvent value) =>
        writer.Write(Encoding.UTF8.GetBytes(value.Payload));
}

/// <summary>Reads nothing, for the same reason as <see cref="ArrayPayloadDeserializer"/>.</summary>
internal sealed class ShapedPayloadDeserializer : IDeserializer<ShapedPayloadEvent>
{
    public ShapedPayloadEvent Deserialize(ReadOnlySpan<byte> data) => new(string.Empty);
}
