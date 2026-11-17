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
    }

    [Test, Timeout(15000)]
    public async Task Bnd7_SameTypeDifferentTag_ThrowsAtTheCall(
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
/// Why this test exists: PicoJetson is CALL-SITE driven. <see cref="JsonlEventStore{TEventBase}"/>
/// serializes through a type PARAMETER — its call sites are <c>SerializeToUtf8Bytes(TEventBase)</c>
/// and <c>Deserialize&lt;TEventBase&gt;</c> — and the generator ignores type parameters, so nothing in
/// <c>PicoActor.Abs</c> (where the two events are declared) and nothing in this store's own store code
/// ever produces a registration for them. The package therefore ships a bare generation point,
/// <c>FrameworkEventSerializers.Generate</c>, whose only job is to hold four CLOSED call sites that
/// name <see cref="SagaCompleted"/> and <see cref="SagaFailed"/> with a concrete type argument.
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
