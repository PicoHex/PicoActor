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

    // ── The counts (D7's boot check reads them; the Info line belongs to B4/B5) ────────────────────

    [Test, Timeout(15000)]
    public async Task Bnd8_Counts(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        using var temp = new TempRoot();
        var options = new JsonlEventStoreOptions();

        // An options instance is EMPTY: exactly one seeding mechanism exists, and it is the constructor.
        await Assert.That(options.EventTypeBindings.RegisteredCount).IsEqualTo(0);
        await Assert.That(options.EventTypeBindings.FrameworkDefaultCount).IsEqualTo(0);

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
