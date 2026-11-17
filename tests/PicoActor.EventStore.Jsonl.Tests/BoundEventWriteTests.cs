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

    // ── Bnd15: the object? member — null (the framework's only shape) round-trips ──────────────────

    [Test, Timeout(15000)]
    public async Task Bnd15_NullResult_RoundTrips_NonNullFailsLoudly(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root);
        var id = Guid.NewGuid();

        // The shape the framework actually produces: every MarkComplete call site is parameterless, so
        // `Result` is null on every completion line a saga stream has ever carried.
        await store.AppendAsync(id, 0, [new SagaCompleted(null)]);

        var lines = await File.ReadAllLinesAsync(StreamPath(temp.Root, id), cancellationToken);
        await Assert.That(lines.Length).IsEqualTo(1);

        // D2's recipe, pinned by literal. If the discriminator were spliced by keeping the payload whole,
        // the line would read `{"$type":"PicoActorSagaCompleted",{"Result":null}` — the payload's own
        // brace kept AND a comma in front of it: invalid JSON, invisible to this write, fatal on
        // read-back. The literal below is the byte string the production corpus already carries (Bnd3
        // pins it from the read side), which is why the payload's opening brace must be DROPPED.
        await Assert.That(lines[0]).IsEqualTo(NullResultLine);

        // Round trip through the CONCRETE deserializer: the extra leading `$type` member is tolerated
        // (D2's invariant), so the line this store writes is a SagaCompleted line and not merely a
        // well-formed one. Reached through a METHOD GROUP on purpose — an invocation here would make the
        // test assembly generate the serializer itself and blind Bnd0's tripwire.
        DeserializeSagaCompleted deserialize = JsonSerializer.Deserialize<SagaCompleted>;
        var nullResult = deserialize(Encoding.UTF8.GetBytes(lines[0]), null);
        await Assert.That(nullResult).IsEqualTo(new SagaCompleted(null));
        await Assert.That(nullResult!.Result).IsNull();

        // ── The non-null half is blocked UPSTREAM, and this is where that shows ────────────────────
        // The spec's D10 fidelity table predicts `Result = "boom"` round-trips as a string. It does not,
        // and this store cannot make it: PicoJetson 2026.13's generated writer emits an EMPTY block for a
        // non-null `object?` member (`if (value.Result != null) { }` — the property name is written with
        // no value), and its generated reader drops such a value too (`// unsupported ctor param type:
        // any`). The append therefore fails inside the serializer, before a byte reaches the disk.
        // What this store owes its operator is asserted rather than the defect: a loud failure and NO
        // stream, never a half-written or unreadable line. When PicoJetson learns to write untyped
        // members this goes RED on purpose — that red is the signal to complete the string round trip
        // the fidelity table promises.
        var otherId = Guid.NewGuid();
        var error = await CaptureAsync(async () =>
        {
            _ = await store.AppendAsync(otherId, 0, [new SagaCompleted("boom")]);
        });

        // "Loud" is pinned, not merely observed: the failure is one of the two documented shapes — a
        // FormatException for a payload that does not fit the tag, an InvalidOperationException for a
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
        // sentence sends the operator looking for something this failure never created. The two legs
        // fail for different reasons (this one is the type's own output, blocked before a byte moves),
        // so one wording cannot serve both.
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
        await Assert.That(File.Exists(StreamPath(temp.Root, otherId))).IsFalse();
        await Assert.That(await store.GetVersionAsync(otherId)).IsEqualTo(0UL);
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
    /// <c>{ }</c> for the same object written by a consumer's
    /// <see cref="JsonSerializer.RegisterCustom{T}"/> serializer. A <c>Length &gt; 2</c> test calls the
    /// second one "has members" and writes <c>{"$type":"t", }</c> — a trailing comma, invalid JSON,
    /// invisible at write time and fatal on read-back. Both cases must produce the tag ALONE.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd18b_MemberlessPayload_IsAlwaysWrittenWithoutASeparator(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;

        // The `{ }` half needs a writer that emits whitespace between the braces; a generated one never
        // does, so this is the consumer hook again.
        JsonSerializer.RegisterCustom(
            new WhitespacePayloadSerializer(),
            new WhitespacePayloadDeserializer()
        );
        // The `{}` half is MilestoneReached's own generated writer (Bnd1b pins that exact path).
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
                new WhitespaceOnlyEvent(),
                "{\"$type\":\"whitespace\"}"
            ),
        ];

        foreach (var (label, boundEvent, expected) in cases)
        {
            using var temp = new TempRoot();
            var options = new JsonlEventStoreOptions();
            // BOTH bindings on every store, so the loop differs only in the event it appends.
            options.EventTypeBindings.Add<MilestoneReached>("memberless");
            options.EventTypeBindings.Add<WhitespaceOnlyEvent>("whitespace");
            var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);
            var id = Guid.NewGuid();

            await store.AppendAsync(id, 0, [boundEvent]);

            var lines = await File.ReadAllLinesAsync(StreamPath(temp.Root, id), cancellationToken);
            await Assert.That(lines.Length).IsEqualTo(1).Because(label);
            await Assert.That(lines[0]).IsEqualTo(expected).Because(label);
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
/// A bound type whose own serializer writes <c>{ }</c> — the SAME memberless object as
/// <see cref="MilestoneReached"/>, with whitespace between the braces (Bnd18b). The splice decides
/// "has members" from the JSON behind the dropped brace, never from the payload's length.
/// </summary>
public sealed record WhitespaceOnlyEvent : IDomainEvent;

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

/// <summary>Writes <c>{ }</c> for every <see cref="WhitespaceOnlyEvent"/>.</summary>
internal sealed class WhitespacePayloadSerializer : ISerializer<WhitespaceOnlyEvent>
{
    public void Serialize(IBufferWriter<byte> writer, WhitespaceOnlyEvent value) =>
        writer.Write("{ }"u8);
}

/// <summary>Reads nothing, for the same reason as <see cref="ArrayPayloadDeserializer"/>.</summary>
internal sealed class WhitespacePayloadDeserializer : IDeserializer<WhitespaceOnlyEvent>
{
    public WhitespaceOnlyEvent Deserialize(ReadOnlySpan<byte> data) => new();
}
