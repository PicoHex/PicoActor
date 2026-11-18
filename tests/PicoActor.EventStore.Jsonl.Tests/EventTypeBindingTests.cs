using System.Buffers;
using PicoJetson;
using PicoLog.Abs;
using PicoSerDe.Core;

namespace PicoActor.EventStore.Jsonl.Tests;

/// <summary>
/// The binding table on its own (D1/D1b): the tag grammar, the one-type-one-tag matrix, the framework
/// defaults the STORE seeds, and the freeze. Every refusal is asserted AT THE CALL, because the
/// operator's next action is to fix the line in their own start-up code — an exception raised later (on
/// the first write, or while reading an archive) would name a symptom instead of the cause.
/// <para>
/// The counts are the other half of the contract: <see cref="EventTypeBindings.RegisteredCount"/> counts
/// every binding on the options instance, <see cref="EventTypeBindings.FrameworkDefaultCount"/> only the
/// defaults that were actually APPLIED, so a consumer override is visible in a number (Bnd14) — and a
/// fresh options instance reporting <c>0</c> is what proves the table is not seeded in an initializer.
/// </para>
/// </summary>
public sealed class EventTypeBindingTests
{
    // ── The matrix (D1b), refused at the call ───────────────────────────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd6_DuplicateTag_ThrowsAtTheCall(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var bindings = new JsonlEventStoreOptions().EventTypeBindings;

        bindings.Add<SagaCompleted>("shared");

        // Same tag, a DIFFERENT type: one discriminator would name two events, so the reader could not
        // tell them apart. The second Add is the offending line, and it fails there.
        var error = Catch(() => bindings.Add<SagaFailed>("shared"));

        await Assert.That(error is InvalidOperationException).IsTrue();
        await Assert.That(error!.Message.Contains("shared", StringComparison.Ordinal)).IsTrue();
        await Assert
            .That(error.Message.Contains(nameof(SagaFailed), StringComparison.Ordinal))
            .IsTrue();

        // The refused call registered nothing.
        await Assert.That(bindings.RegisteredCount).IsEqualTo(1);

        // The idempotence row: the SAME type with the SAME tag is a no-op, never an error.
        bindings.Add<SagaCompleted>("shared");
        await Assert.That(bindings.RegisteredCount).IsEqualTo(1);

        // …and "the same tag" means what the READER means by it. The generated polymorphic reader
        // matches a discriminator value case-insensitively, so two spellings of one tag are one tag:
        // re-registering in another case is the same idempotent no-op, and the spelling registered
        // FIRST stays the wire format (Bnd4b pins that on the read side).
        bindings.Add<SagaCompleted>("SHARED");
        await Assert.That(bindings.RegisteredCount).IsEqualTo(1);
    }

    [Test, Timeout(15000)]
    public async Task Bnd6b_SameTypeDifferentTag_ThrowsAtTheCall(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var bindings = new JsonlEventStoreOptions().EventTypeBindings;

        bindings.Add<SagaCompleted>("first");

        // One type, two tags: a stream becomes unreadable the moment the other tag is written into it,
        // so the second registration is refused where it is written.
        var error = Catch(() => bindings.Add<SagaCompleted>("second"));

        await Assert.That(error is InvalidOperationException).IsTrue();

        // BOTH tags are named: the operator has to know which binding to delete.
        await Assert.That(error!.Message.Contains("first", StringComparison.Ordinal)).IsTrue();
        await Assert.That(error.Message.Contains("second", StringComparison.Ordinal)).IsTrue();

        await Assert.That(bindings.RegisteredCount).IsEqualTo(1);
    }

    // ── A bound type that is ALSO a TEventBase: refused at CONSTRUCTION, before the freeze (D5) ────

    /// <summary>
    /// D5's second half, and the one a consumer actually reaches: binding the store's OWN event (here
    /// <see cref="SSnapshotAdded"/>, which derives from <see cref="ConformanceEvent"/>) would give one
    /// type two routes — the base's <c>[PicoDerivedType]</c> tag and the bound tag — so a type is
    /// registered once and the store refuses the construction.
    /// <para>
    /// What this pins is WHERE the refusal lands and WHAT it leaves behind. It has to land before the
    /// table is frozen, because a refused construction that froze the options would turn the documented
    /// one-line fix ("delete the Add call") into a dead options instance: no binding can be added to it
    /// afterwards and constructing over it again still throws, so the operator has no way forward in
    /// the same process. And it must leave the COUNTS as it found them, because D7 says a construction
    /// that throws attests to nothing — the framework seeds it applied are rolled back with it, not
    /// counted against a store that does not exist.
    /// </para>
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd7_BoundTypeAlsoTEventBase_ThrowsAtConstruction(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions();

        // SSnapshotAdded IS a ConformanceEvent, so this binding registers a type the base already
        // routes — the ambiguity, spelled the way an operator spells it.
        options.EventTypeBindings.Add<SSnapshotAdded>("ss-bound");

        var error = Catch(() => _ = new JsonlEventStore<ConformanceEvent>(temp.Root, options));

        await Assert.That(error is InvalidOperationException).IsTrue();

        var message = error!.Message;

        // Both names: WHICH binding line is wrong, and WHICH generic argument it collides with.
        await Assert
            .That(message.Contains(typeof(SSnapshotAdded).FullName!, StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(message.Contains(typeof(ConformanceEvent).FullName!, StringComparison.Ordinal))
            .IsTrue();

        // Exactly ONE remedy, and it is the one that works: delete the registration, because the base
        // already covers the type. The other half the message used to offer — "register the event's own
        // base type as the store's generic argument" — is already the case here (the generic argument
        // IS the type's base) and fails again, so it must not be offered at all.
        await Assert
            .That(message.Contains($"Add<{nameof(SSnapshotAdded)}>", StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(
                message.Contains(
                    "the event's base type already covers it",
                    StringComparison.Ordinal
                )
            )
            .IsTrue();
        await Assert
            .That(message.Contains("register the event's own base type", StringComparison.Ordinal))
            .IsFalse();

        // Refused before the stream: no file, no directory entry.
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);

        // ── the observable surface a refused construction must leave usable ────────────────────────
        // NOT frozen. This is the line the fix exists for: over the same options instance, the operator
        // can still register a binding, which is only possible because the freeze never completed.
        options.EventTypeBindings.Add<SagaFailed>("y");

        // The refused construction seeded NOTHING either: the two consumer bindings are all there is,
        // so the framework defaults it would have applied were rolled back with it (D7).
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(0);
    }

    // ── The store seeds its own defaults; a consumer override survives (D1) ────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd14_ConsumerOverrideWins(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions();

        // The consumer binds the framework's completion event BEFORE the store exists.
        options.EventTypeBindings.Add<SagaCompleted>("Acme");

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);

        // The FROZEN table the store reads is what must carry "Acme" — that is the tag the append path
        // splices and the read path dispatches on, not the default.
        await Assert.That(store.Bindings.TryGetTag(typeof(SagaCompleted), out var tag)).IsTrue();
        await Assert.That(tag).IsEqualTo("Acme");

        // The OTHER default still applied: TryAdd semantics, never all-or-nothing.
        await Assert.That(store.Bindings.TryGetTag(typeof(SagaFailed), out var failedTag)).IsTrue();
        await Assert.That(failedTag).IsEqualTo(FrameworkEventTags.SagaFailed);

        // ONE default applied, two bindings in total — the counts the boot check reads.
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(1);
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(store.Bindings.FrameworkDefaultCount).IsEqualTo(1);
        await Assert.That(store.Bindings.Count).IsEqualTo(2);
    }

    // ── The tag grammar: a tag is spliced into the line verbatim (D2/D1b) ──────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd16_TagGrammar_RejectsQuotesBackslashAndBlanks(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        var bindings = new JsonlEventStoreOptions().EventTypeBindings;

        // A tag lands between the quotes of `{"$type":"<tag>"` with no escaping, so anything that would
        // close the string early — a quote, a backslash, a control character — or name nothing at all is
        // refused HERE, not on read-back (where the damage may already sit in an archive).
        string[] invalid =
        [
            string.Empty,
            " ",
            "   ",
            "tab\t",
            "quote\"here",
            "back\\slash",
            "line\nfeed",
            "nul\u0000byte",
            "esc\u001bescape",
        ];

        foreach (var tag in invalid)
        {
            await Assert
                .That(Catch(() => bindings.Add<SagaCompleted>(tag)) is ArgumentOutOfRangeException)
                .IsTrue();
        }

        // A null tag is the argument-shape refusal, distinct from the grammar one.
        await Assert
            .That(Catch(() => bindings.Add<SagaCompleted>(null!)) is ArgumentNullException)
            .IsTrue();

        // Nothing was registered by any of the refusals...
        await Assert.That(bindings.RegisteredCount).IsEqualTo(0);

        // ...and the grammar is not refusing everything: the canonical tag is accepted.
        bindings.Add<SagaCompleted>(FrameworkEventTags.SagaCompleted);
        await Assert.That(bindings.RegisteredCount).IsEqualTo(1);
    }

    // ── The freeze (D8): the first construction is the last word ───────────────────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd17_AddAfterFreeze_Throws(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        using var secondRoot = new TempRoot();
        var options = new JsonlEventStoreOptions();

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);

        // The store froze the table, so a late Add would silently not apply to it. It is refused, and the
        // message names the construction that froze it instead of leaving the operator with a binding
        // that "worked" and a store that never saw it.
        var error = Catch(() => options.EventTypeBindings.Add<SagaCompleted>("acme"));

        await Assert.That(error is InvalidOperationException).IsTrue();
        await Assert
            .That(
                error!.Message.Contains(
                    nameof(JsonlEventStore<ConformanceEvent>),
                    StringComparison.Ordinal
                )
            )
            .IsTrue();

        // The live store's table is untouched by the refused call.
        await Assert.That(store.Bindings.Count).IsEqualTo(2);

        // The tag GRAMMAR is checked first, so an illegal tag is still diagnosed as an illegal tag on
        // a frozen table: the operator who typed a blank tag is not asked to work out that a store
        // exists as well. The freeze refusal belongs to the tag that could have been registered.
        await Assert
            .That(
                Catch(() => options.EventTypeBindings.Add<SagaCompleted>(" "))
                    is ArgumentOutOfRangeException
            )
            .IsTrue();

        // A SECOND construction over the same options does not seed again: the defaults were applied
        // once, inside the critical section that froze the table.
        var secondStore = new JsonlEventStore<ConformanceEvent>(secondRoot.Root, options);

        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(secondStore.Bindings.Count).IsEqualTo(2);
    }

    // ── The counts AND the start-up line (D7): one self-attesting Info line per store ──────────────

    [Test, Timeout(15000)]
    public async Task Bnd8_Counts(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var logger = new RecordingLogger();
        var options = new JsonlEventStoreOptions { Logger = logger };

        // An options instance is EMPTY: exactly one seeding mechanism exists, and it is the constructor.
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(0);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(0);

        // The line belongs to the CONSTRUCTOR, so an unfrozen table has nothing to attest yet.
        await Assert.That(logger.Entries.Count).IsEqualTo(0);

        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);

        // Two framework defaults, both applied, and no consumer binding.
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(2);
        await Assert.That(store.Bindings.Count).IsEqualTo(2);
        await Assert.That(store.Bindings.FrameworkDefaultCount).IsEqualTo(2);

        // The canonical tags are permanent wire format, so they are pinned by literal here — the corpus
        // already carries them (2,403 + 6 records), and an alias would have no way to rename them.
        await Assert
            .That(store.Bindings.TryGetTag(typeof(SagaCompleted), out var completed))
            .IsTrue();
        await Assert.That(completed).IsEqualTo("PicoActorSagaCompleted");
        await Assert.That(store.Bindings.TryGetTag(typeof(SagaFailed), out var failed)).IsTrue();
        await Assert.That(failed).IsEqualTo("PicoActorSagaFailed");

        // The reverse lookup the READ path needs agrees with it.
        await Assert
            .That(store.Bindings.TryGetEventType("PicoActorSagaCompleted", out var completedType))
            .IsTrue();
        await Assert.That(completedType).IsEqualTo(typeof(SagaCompleted));
        await Assert
            .That(store.Bindings.TryGetEventType("PicoActorSagaFailed", out var failedType))
            .IsTrue();
        await Assert.That(failedType).IsEqualTo(typeof(SagaFailed));

        // An unbound tag and an unbound type are simply NOT FOUND — a miss is never a throw.
        await Assert.That(store.Bindings.TryGetEventType("who-knows", out _)).IsFalse();
        await Assert.That(store.Bindings.TryGetTag(typeof(ConformanceEvent), out _)).IsFalse();

        // ── D7: the start-up self-attestation ──────────────────────────────────────────────────────
        // One store, ONE line: an operator reading a boot log must be able to tell what this process
        // routes from the log alone, without a probe. The text is pinned by literal because it is the
        // contract the boot check reads, and the numbers in it must be the FROZEN counts.
        await Assert.That(logger.Entries.Count).IsEqualTo(1);

        var info = logger.Entries.Where(entry => entry.Level == LogLevel.Info).ToList();
        await Assert.That(info.Count).IsEqualTo(1);
        await Assert
            .That(info[0].Message)
            .IsEqualTo("event type bindings: 2 registered (2 framework-default)");
    }

    // ── Bnd19: a bound tag the BASE's own registration already declares: refused at construction (D5) ─

    /// <summary>
    /// The collision that produced a SILENT misread. <see cref="ConformanceEvent"/> declares
    /// <c>[PicoDerivedType(typeof(SSnapshotAdded), "ss-added")]</c>, so a consumer who bound some OTHER
    /// type to the tag <c>"ss-added"</c> got a store that wrote its lines happily — and, because the READ
    /// path asks the bindings FIRST, read back every <c>ss-added</c> line the base had already written as
    /// that other type, losing <see cref="SSnapshotAdded"/> and its members. Nothing on the write side can
    /// notice; the construction refuses instead.
    /// <para>
    /// Binding <see cref="SSnapshotAdded"/> itself is refused EARLIER, by the assignability check (Bnd7), so
    /// the collision has to be spelled with a type that cannot derive from the base:
    /// <see cref="ForeignEvent"/>. The refusal lands in the construction-time validation — BEFORE the freeze
    /// is committed — so the options instance stays usable, exactly like Bnd7's.
    /// </para>
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd19_BoundTagClashesWithADerivedTag_ThrowsAtConstruction(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions();

        options.EventTypeBindings.Add<ForeignEvent>("ss-added");

        var error = Catch(() => _ = new JsonlEventStore<ConformanceEvent>(temp.Root, options));

        await Assert.That(error is InvalidOperationException).IsTrue();

        var message = error!.Message;

        // The tag, and the fact that it ALREADY names something on the base — the operator's next action
        // is to rename one of the two, so both facts have to be in the refusal.
        await Assert.That(message.Contains("\"ss-added\"", StringComparison.Ordinal)).IsTrue();
        await Assert.That(message.Contains("already names", StringComparison.Ordinal)).IsTrue();

        // …and WHICH base declares it, because the tag lives in an assembly the consumer may not own.
        await Assert
            .That(message.Contains(typeof(ConformanceEvent).FullName!, StringComparison.Ordinal))
            .IsTrue();
        await Assert
            .That(message.Contains(typeof(ForeignEvent).FullName!, StringComparison.Ordinal))
            .IsTrue();

        // Refused before the stream and before the freeze: no file, and the same options instance still
        // takes a binding with the framework seeds rolled back (D7, the Bnd7 contract).
        await Assert.That(Directory.GetFiles(temp.Root).Length).IsEqualTo(0);

        options.EventTypeBindings.Add<SagaFailed>("y");
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(0);
    }

    // ── Bnd19b: the RESIDUAL HOLE — a derived reader that recognises the tag and then THROWS ───────

    /// <summary>
    /// The hole the guard cannot see, pinned as a FACT. The probe asks the base's polymorphic reader about
    /// the tag, and takes "the call THREW" to mean "this tag is not claimed". <see cref="GuardedEventBase"/>
    /// declares <c>[PicoDerivedType(typeof(GuardedDeltaEvent), "hole-tag")]</c>, whose reader RECOGNISES the
    /// tag and then rejects the memberless payload — its constructor refuses the default arguments the probe
    /// supplies. So the probe throws for a reason that is NOT "unknown tag", the guard reads it as free, and
    /// a binding on <c>hole-tag</c> is accepted.
    /// <para>
    /// The damage is the one the guard exists to prevent, in the direction it really happens: the READ path
    /// asks the bindings first, so the base's own <c>hole-tag</c> lines — written by the base's own writer,
    /// readable by the base's own reader — come back from <c>LoadAsync</c> as the BOUND type, with
    /// <see cref="GuardedDeltaEvent"/> and its <c>Delta</c> gone. The public API has no separate "is this
    /// discriminator declared?" signal (the reader is the only oracle and answers with an event or an
    /// exception), so this is DOCUMENTED behaviour, not a bug to re-report: the test exists so a later
    /// reader of the suite finds the known hole stated instead of rediscovering it as a new one.
    /// </para>
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd19b_ResidualHole_DerivedReaderThrowsOnEmptyObject_IsDocumented(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();

        // The consumer hook that gives the bound type its serializer: this type is deliberately NOT a
        // GuardedEventBase, so the assignability refusal (Bnd7) cannot be what answers here.
        JsonSerializer.RegisterCustom(new HoleBoundSerializer(), new HoleBoundDeserializer());

        var options = new JsonlEventStoreOptions();
        options.EventTypeBindings.Add<HoleBoundEvent>("hole-tag");

        // The mechanism, asserted rather than described: the very probe the guard runs THROWS for this
        // tag — and not because the tag is unknown, which would be the FormatException the guard looks
        // for. That is precisely the answer it has to read as "not claimed".
        var probe = Catch(() =>
        {
            _ = JsonSerializer.Deserialize<GuardedEventBase>(
                Encoding.UTF8.GetBytes("{\"$type\":\"hole-tag\"}")
            );
        });
        await Assert.That(probe is ArgumentOutOfRangeException).IsTrue();

        // The hole, as a fact: the probe's throw is read as "this tag is not claimed", so the binding is
        // ACCEPTED. Construction succeeds — this is the documented behaviour, not an expectation.
        var store = new JsonlEventStore<GuardedEventBase>(temp.Root, options);

        // The line the BASE writes for its own event, produced by its own polymorphic writer…
        var baseLine = JsonSerializer.SerializeToUtf8Bytes<GuardedEventBase>(
            new GuardedDeltaEvent(5)
        );

        // …and decoded by its own polymorphic reader, member and all: the tag really is the base's.
        var baseEvent = JsonSerializer.Deserialize<GuardedEventBase>(baseLine);
        await Assert.That(baseEvent).IsEqualTo(new GuardedDeltaEvent(5));

        // Put that line in a stream this store reads.
        var id = Guid.NewGuid();
        await File.WriteAllTextAsync(
            StreamPath(temp.Root, id),
            Encoding.UTF8.GetString(baseLine) + "\n",
            cancellationToken
        );

        var loaded = await store.LoadAsync(id);

        // THE HOLE: the read path asks the binding FIRST, so the base's own line comes back as the BOUND
        // type. GuardedDeltaEvent, its Delta and the fact that the line was ever a base event are all
        // gone, with nothing thrown and nothing logged — the substitution the guard exists to refuse.
        await Assert.That(loaded.Count).IsEqualTo(1);
        await Assert.That(loaded[0].GetType()).IsEqualTo(typeof(HoleBoundEvent));
    }

    // ── Bnd20: a log SINK that throws cannot poison the table the construction just froze (M8) ─────

    /// <summary>
    /// The start-up attestation is the LAST thing the constructor does, and by then the bindings are
    /// FROZEN and copied: a sink that throws (a full disk, a closed connection, a broken formatter) would
    /// otherwise turn a completed, usable store into a failed construction AND leave the operator with an
    /// options instance that accepts no further binding — a store that never existed, over options that can
    /// never be fixed. A logger is foreign code, so its failure stays its own.
    /// </summary>
    [Test, Timeout(15000)]
    public async Task Bnd20_ThrowingLogger_DoesNotPoisonTheConstruction(
        CancellationToken cancellationToken = default
    )
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions { Logger = new ThrowingLogger() };

        // The whole point: construction SUCCEEDS although every log call it makes throws.
        var store = new JsonlEventStore<ConformanceEvent>(temp.Root, options);

        // The frozen table was committed WHOLE (not rolled back by the sink's failure), in both directions.
        await Assert.That(store.Bindings.Count).IsEqualTo(2);
        await Assert.That(store.Bindings.FrameworkDefaultCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(2);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(2);

        // …and the store is USABLE: a mixed batch round-trips through both routes.
        var id = Guid.NewGuid();
        await store.AppendAsync(id, 0, [new SSnapshotAdded(3), new SagaCompleted(null)]);

        var loaded = await store.LoadAsync(id);
        await Assert.That(loaded.Count).IsEqualTo(2);
        await Assert.That(loaded[0]).IsEqualTo(new SSnapshotAdded(3));
        await Assert.That(loaded[1]).IsEqualTo(new SagaCompleted(null));

        // The table is frozen, as it always is once a store exists (Bnd17): the sink's failure changed
        // nothing about that, and the refusal names the construction as usual.
        await Assert
            .That(
                Catch(() => options.EventTypeBindings.Add<SagaCompleted>("acme"))
                    is InvalidOperationException
            )
            .IsTrue();
    }

    /// <summary>The path the default (<see cref="FileNameFormat.IdOnly"/>) format gives one stream.</summary>
    private static string StreamPath(string root, Guid id) =>
        Path.Combine(root, id.ToString("D") + ".jsonl");

    /// <summary>Run <paramref name="action"/> and hand back what it threw, or <c>null</c>.</summary>
    private static Exception? Catch(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}

/// <summary>
/// Bnd0 — the framework's two saga terminal events must have a serializer SOMEBODY generates, and the
/// somebody is this store package (D2b of the event-type-bindings spec).
/// <para>
/// Why this test exists: PicoSerDe generation is CALL-SITE driven. <see cref="JsonlEventStore{TEventBase}"/>
/// serializes through a type PARAMETER — its call sites are <c>SerializeToUtf8Bytes(TEventBase)</c>
/// and <c>Deserialize&lt;TEventBase&gt;</c> — and the generator ignores type parameters, so nothing in
/// <c>PicoActor.Abs</c> (where the two events are declared) and nothing in this store's own store code
/// ever produces a registration for them. The package therefore ships a bare generation point,
/// <c>FrameworkEventSerializers</c>, whose only job is to carry the two attributes
/// (<c>[GenerateSerializer(typeof(SagaCompleted))]</c> / <c>…(typeof(SagaFailed))</c>) that name those
/// types CONCRETELY.
/// </para>
/// <para>
/// Why the serializers are reached through DELEGATES: this test must fail when that generation point
/// is deleted, and a candidate call site in THIS assembly would defeat it. Every invocation named
/// Serialize / SerializeToUtf8Bytes / Deserialize / … with exactly one type argument makes the
/// generator emit a serializer for that argument in the CALLING assembly, and the registration spread
/// is process-wide — the test would then pass on the test assembly's own registration and prove
/// nothing. A method-group
/// conversion (<c>= JsonSerializer.SerializeToUtf8Bytes&lt;SagaCompleted&gt;</c>) is not an
/// invocation, so it generates nothing, while the delegate still goes through the very same public
/// facade the store uses. What is observed below is therefore only this: is a serializer registered
/// for these two types by SOMEONE ELSE's assembly?
/// </para>
/// </summary>
public class FrameworkSerializerTripswireTests
{
    private delegate SagaCompleted? DeserializeSagaCompleted(
        ReadOnlySpan<byte> data,
        JsonOptions? options = null
    );

    private delegate SagaFailed? DeserializeSagaFailed(
        ReadOnlySpan<byte> data,
        JsonOptions? options = null
    );

    [Test]
    public async Task Bnd0_FrameworkEventSerializer_IsGenerated()
    {
        // Touching a type of the store assembly runs its module initializer, which is where the
        // generated registrations of this package are installed.
        _ = new JsonlEventStoreOptions();

        // Create the delegates FIRST: on the unfixed tree this is where "No serializer/deserializer
        // registered for …SagaCompleted" is reported, at the first use below.
        Func<SagaCompleted, JsonOptions?, byte[]>? serializeCompleted =
            JsonSerializer.SerializeToUtf8Bytes<SagaCompleted>;
        DeserializeSagaCompleted? deserializeCompleted = JsonSerializer.Deserialize<SagaCompleted>;
        Func<SagaFailed, JsonOptions?, byte[]>? serializeFailed =
            JsonSerializer.SerializeToUtf8Bytes<SagaFailed>;
        DeserializeSagaFailed? deserializeFailed = JsonSerializer.Deserialize<SagaFailed>;

        // The registrations themselves, so a missing one fails with its own name instead of a
        // downstream round-trip mismatch.
        await Assert.That(SerRegistry<JsonFormat, SagaCompleted>.Handler).IsNotNull();
        await Assert.That(DesRegistry<JsonFormat, SagaCompleted>.Deserializer).IsNotNull();
        await Assert.That(SerRegistry<JsonFormat, SagaFailed>.Handler).IsNotNull();
        await Assert.That(DesRegistry<JsonFormat, SagaFailed>.Deserializer).IsNotNull();

        // Serialize + deserialize the null-result completion — the payload a saga stream carries.
        var completed = new SagaCompleted(null);
        var completedBytes = serializeCompleted!(completed, null);
        var completedJson = Encoding.UTF8.GetString(completedBytes);
        var completedBack = deserializeCompleted!(completedBytes, null);
        await Assert.That(completedJson).IsEqualTo("{\"Result\":null}");
        await Assert.That(completedBack).IsEqualTo(completed);

        // …and the failure event, whose single member is a plain string.
        var failed = new SagaFailed("boom");
        var failedBytes = serializeFailed!(failed, null);
        var failedJson = Encoding.UTF8.GetString(failedBytes);
        var failedBack = deserializeFailed!(failedBytes, null);
        await Assert.That(failedJson).IsEqualTo("{\"Reason\":\"boom\"}");
        await Assert.That(failedBack).IsEqualTo(failed);
    }
}

/// <summary>
/// Bnd19b's base: its ONE registration names a derived event that VALIDATES its arguments, so the
/// construction-time tag probe — which deserializes a memberless discriminator-only object — constructs
/// that event with defaults and the constructor throws. The probe therefore cannot tell "this tag is
/// unknown" from "this tag is declared and undecodable", which is the residual hole Bnd19b documents.
/// </summary>
[PicoDerivedType(typeof(GuardedDeltaEvent), "hole-tag")]
public abstract record GuardedEventBase : IDomainEvent;

/// <summary>
/// An ordinary domain event with an invariant — a delta is positive. Nothing exotic: a guard clause in a
/// domain event is idiomatic, and it is all it takes to make the base's reader throw for a tag it
/// recognises. A payload that CARRIES the member decodes normally (Bnd19b asserts that half too).
/// </summary>
public sealed record GuardedDeltaEvent(int Delta) : GuardedEventBase
{
    public int Delta { get; init; } =
        Delta > 0
            ? Delta
            : throw new ArgumentOutOfRangeException(nameof(Delta), "a delta must be positive");
}

/// <summary>The type Bnd19b binds to <c>hole-tag</c>: an <see cref="IDomainEvent"/> and nothing else.</summary>
public sealed record HoleBoundEvent() : IDomainEvent;

/// <summary>Writes <c>{}</c> for every <see cref="HoleBoundEvent"/> — the write leg is not Bnd19b's subject.</summary>
internal sealed class HoleBoundSerializer : ISerializer<HoleBoundEvent>
{
    public void Serialize(IBufferWriter<byte> writer, HoleBoundEvent value) => writer.Write("{}"u8);
}

/// <summary>
/// Reads nothing and answers a fresh event: Bnd19b asks which TYPE the store hands back for a line the
/// base wrote, so the binding's reader must succeed on whatever bytes it is given.
/// </summary>
internal sealed class HoleBoundDeserializer : IDeserializer<HoleBoundEvent>
{
    public HoleBoundEvent Deserialize(ReadOnlySpan<byte> data) => new();
}

/// <summary>
/// A logger that FAILS on every call (Bnd20). A sink is foreign code: the store hands it a finished
/// sentence and reads nothing back, so an exception from it must never reach the store's own state.
/// </summary>
internal sealed class ThrowingLogger : ILogger
{
    public IDisposable BeginScope<TState>(TState state)
        where TState : notnull => throw new InvalidOperationException("the sink is down");

    public void Log(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => throw new InvalidOperationException("the sink is down");

    public Task LogAsync(
        LogLevel logLevel,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    ) => throw new InvalidOperationException("the sink is down");

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => throw new InvalidOperationException("the sink is down");

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        string message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    ) => throw new InvalidOperationException("the sink is down");

    public void Log(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => throw new InvalidOperationException("the sink is down");

    public Task LogAsync(
        LogLevel logLevel,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    ) => throw new InvalidOperationException("the sink is down");

    public void Log(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception
    ) => throw new InvalidOperationException("the sink is down");

    public Task LogAsync(
        LogLevel logLevel,
        EventId eventId,
        FormattableString message,
        IReadOnlyList<KeyValuePair<string, object?>>? properties,
        Exception? exception,
        CancellationToken cancellationToken
    ) => throw new InvalidOperationException("the sink is down");
}
