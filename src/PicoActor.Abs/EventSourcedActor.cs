namespace PicoActor.Abs;

/// <summary>
/// Base class for Event-Sourced actors.
/// Inherits Actor; adds RaiseEvent, event management, version tracking, and persistence.
///
/// Follows the Persist-then-Mutate pattern:
///   OnMessageAsync → RaiseEvent (record only, no state change)
///   → Persist to IEventStore
///   → Mutate (apply events to in-memory state)
///   → CommitEvents
///
/// State is only mutated after successful persistence, guaranteeing that
/// in-memory state is always consistent with the event stream.
/// </summary>
public abstract class EventSourcedActor : Actor, IEventSourcedActor, ISnapshotableActor
{
    private readonly List<IDomainEvent> _events = new();

    /// <inheritdoc/>
    public ulong Version { get; protected set; }

    /// <summary>
    /// True while this instance is a rebuild whose state came from the persisted stream
    /// (<see cref="IEventSourcedActor.ReplayEvents"/>) rather than from a live creation
    /// command. Set by replay, cleared by <see cref="OnInitialized"/> once initialization
    /// finishes. "Version &gt; 0" is NOT a rebuild marker: a subclass may raise events in
    /// its constructor, which is a creation.
    /// </summary>
    /// <remarks>
    /// REPLAY IS NOT A LIVE DISPATCH. Effects that are O(n) per event — publishing the read
    /// model (a database write, a projection update, an outbound notification), messaging
    /// another actor, logging at Info — must be skipped while this is true. Replaying n
    /// events with an O(n) effect per event makes the rebuild O(n^2), which is exactly what
    /// makes long-lived aggregates slow to recover. One-shot effects are not the problem;
    /// the per-event ones are.
    /// <para>
    /// Read it inside <see cref="Mutate"/>, not in a constructor: the flag is set by replay
    /// and cleared when initialization completes, so a live dispatch after recovery sees
    /// false.
    /// </para>
    /// </remarks>
    protected bool IsReplaying { get; private set; }

    /// <summary>
    /// May be null in tests or non-persistent scenarios (pure in-memory mode).
    /// </summary>
    private IEventStore? _eventStore;

    /// <summary>Publish hook; default null (no publishing).</summary>
    private IDomainEventPublisher? _publisher;

    /// <summary>
    /// Snapshot trigger: save a snapshot once this many events have been flushed since the
    /// last snapshot attempt. Null = snapshotting off (the default, and the only value unless
    /// the host opts in through <c>ActorSystemOptions.SnapshotEveryNEvents</c>).
    /// </summary>
    private int? _snapshotEveryNEvents;

    /// <summary>Events flushed since the last snapshot attempt.</summary>
    private int _eventsSinceSnapshot;

    /// <summary>
    /// Framework wiring: attaches the event store and the publish hook.
    /// Called by IActorSystem via <see cref="Actor.AttachToSystem"/>'s flow, before
    /// SignalReady(). Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void AttachPersistence(IEventStore? eventStore, IDomainEventPublisher? publisher)
    {
        _eventStore = eventStore;
        _publisher = publisher;
    }

    /// <summary>
    /// Framework wiring: attaches the snapshot policy chosen by the host
    /// (<c>ActorSystemOptions.SnapshotEveryNEvents</c>) and resets the counter. Called by
    /// IActorSystem through <see cref="Actor.AttachToSystem"/>'s flow, before SignalReady().
    /// Not for application code.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void AttachSnapshotPolicy(int? everyNEvents)
    {
        _snapshotEveryNEvents = everyNEvents;
        _eventsSinceSnapshot = 0;
    }

    /// <summary>Creation path. Chains to Actor(ICommand).</summary>
    protected EventSourcedActor(ICommand creationCommand)
        : base(creationCommand) { }

    /// <summary>Rebuild path. Chains to Actor().</summary>
    protected EventSourcedActor() { }

    /// <summary>
    /// Record a domain event and increment Version.
    /// Does NOT mutate state — Mutate is called later, after successful persistence.
    /// Called by subclasses within OnMessageAsync.
    /// </summary>
    protected void RaiseEvent<T>(T @event)
        where T : notnull, IDomainEvent
    {
        _events.Add(@event);
        Version++;
    }

    /// <summary>
    /// Mutate in-memory state from a domain event.
    /// Called by FlushEventsAsync (after persistence) and ReplayEvents (recovery).
    /// Must be a pure function — no validation, no side effects.
    /// Throwing is a FATAL actor fault: the batch is already durable and cannot be
    /// un-applied, so the actor is stopped (fail loud) instead of continuing with
    /// untrusted state or retrying the leaked batch on the next flush.
    /// </summary>
    protected abstract void Mutate(IDomainEvent @event);

    /// <summary>
    /// Framework event handling hook: when it returns true, the event is handled
    /// internally by the framework and is NOT delivered to the subclass Mutate.
    /// Base default is false (all events go to Mutate); SagaActor overrides it to
    /// handle SagaCompleted/SagaFailed. Effective on both paths:
    /// FlushEventsAsync (after persistence) and ReplayEvents (recovery).
    /// </summary>
    protected virtual bool TryHandleFrameworkEvent(IDomainEvent @event) => false;

    /// <summary>
    /// May the runtime reclaim this instance's materialized state while it is idle, and rebuild
    /// it from the event stream on the next lookup? Default <see langword="true"/>: for a plain
    /// aggregate the in-memory state is only a cache and the stream is the truth.
    /// <para>
    /// Override to <see langword="false"/> when being idle is the actor's JOB rather than
    /// evidence that nothing is happening — <see cref="SagaActor"/>, which waits for external
    /// replies, is the case this exists for. Reclaiming such an actor hands its id back to nobody
    /// (Send/AskAsync then throw <see cref="KeyNotFoundException"/>) exactly when the outside
    /// world is about to answer it, and the coordination state is stranded mid-flight.
    /// </para>
    /// <para>
    /// This is a property of the actor TYPE for the runtime, not host configuration (the host
    /// opts the whole feature in or out with
    /// <c>ActorSystemOptions.IdleTimeout</c>). Returning false only exempts the actor from
    /// passivation; nothing else about it changes. Not for application code.
    /// </para>
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public virtual bool CanBePassivated => true;

    /// <summary>
    /// State-shape tag stamped into the snapshot payload, so recovery never applies bytes
    /// written by a DIFFERENT shape. <see langword="null"/> (the base default) disables the
    /// envelope: the payload is stored and restored byte-for-byte, exactly as before.
    /// <para>
    /// <b>Bump this whenever the state shape changes</b> (a field added, removed, or re-typed, or
    /// an existing field whose meaning changed). Changing it invalidates older snapshots ONCE:
    /// the next rebuild replays the whole stream, and normal speed returns after the aggregate
    /// has flushed another <c>SnapshotEveryNEvents</c> events, which overwrites the stale
    /// payload. Nothing is deleted and nothing is lost — history is never truncated.
    /// </para>
    /// <para>
    /// Only <see langword="null"/> disables the envelope. <c>""</c> is a REAL tag: it is written
    /// with <c>schemaLen = 0</c> and validated like any other, so <c>string.IsNullOrEmpty</c> must
    /// never stand in for the null check on either side (that would write raw and then reject its
    /// own payload forever).
    /// </para>
    /// <para>
    /// <b>The tag must be a property of the TYPE, not of per-instance injected state.</b> The
    /// value read by a rebuild instance is compared with the value the writing instance
    /// produced, so when it does come from a field, the create factory and the rebuild factory
    /// must set it identically — a mismatch turns EVERY recovery into a full replay, with only a
    /// warning as evidence.
    /// </para>
    /// <para>
    /// The envelope is framework-owned and lives inside the opaque payload
    /// (<c>"PAS1" + 1-byte UTF-8 length + tag + payload</c>), so stores and
    /// <see cref="ActorSnapshot"/> are unchanged. An <see cref="ISnapshotableActor"/> implemented
    /// OUTSIDE this base class gets no help here: it would have to unwrap the envelope itself,
    /// i.e. slice past the header before handing the bytes to its own restore logic.
    /// </para>
    /// <para>
    /// Keep tags plain ASCII/UTF-8 text of at most 255 UTF-8 bytes: a longer tag cannot be
    /// length-prefixed in one byte, so snapshotting is skipped for that type (and the failure is
    /// reported through <see cref="OnSnapshotFailed"/> on every threshold). A tag containing a
    /// lone surrogate never round-trips, because encoding replaces it.
    /// </para>
    /// </summary>
    protected virtual string? SnapshotSchema => null;

    /// <summary>
    /// The 4-byte marker that opens a framework-owned snapshot envelope. It is the CONTAINER
    /// version and never changes: a future format revision adds fields inside the tag, so an
    /// older binary keeps reading an unknown payload as "no envelope" instead of half-parsing
    /// it.
    /// </summary>
    private static ReadOnlySpan<byte> SnapshotMagic => "PAS1"u8;

    /// <summary>
    /// Wrap <paramref name="payload"/> into <c>[magic][schemaLen][schema][payload]</c>, where
    /// <paramref name="schema"/> is encoded as UTF-8 and <c>schemaLen</c> is its BYTE count
    /// (0..255).
    /// <para>
    /// Returns a NEW array: the buffer <see cref="TakeSnapshot"/> returned may be a reusable one,
    /// and shifting its bytes in place would corrupt the producer's next payload.
    /// </para>
    /// </summary>
    private static byte[] WrapSnapshot(string schema, byte[] payload)
    {
        var schemaLength = Encoding.UTF8.GetByteCount(schema);
        var wrapped = new byte[SnapshotMagic.Length + 1 + schemaLength + payload.Length];
        SnapshotMagic.CopyTo(wrapped);
        wrapped[SnapshotMagic.Length] = (byte)schemaLength;
        Encoding.UTF8.GetBytes(schema, wrapped.AsSpan(SnapshotMagic.Length + 1, schemaLength));
        payload.CopyTo(wrapped, SnapshotMagic.Length + 1 + schemaLength);
        return wrapped;
    }

    /// <summary>
    /// Produce the state this aggregate is fully described by, at the current
    /// <see cref="Version"/>. Return null (the base default) to opt out: the type does not
    /// snapshot, and recovery keeps replaying the whole stream.
    /// <para>
    /// Called only after a successful flush that reached the configured threshold, so the
    /// state it reads is the state the stream describes at <see cref="Version"/>. Snapshotting
    /// is an ACCELERATOR: the event stream is never truncated, so returning null, changing the
    /// payload shape, or failing here only costs speed. Throwing is reported through
    /// <see cref="OnSnapshotFailed"/> and never fails the flushed command.
    /// </para>
    /// <para>
    /// <b>Override this TOGETHER WITH <see cref="RestoreSnapshot"/>.</b> Returning a payload that
    /// <see cref="RestoreSnapshot"/> cannot apply is a contract error: recovery refuses the
    /// snapshot (see there) and replays the whole stream.
    /// </para>
    /// </summary>
    protected virtual byte[]? TakeSnapshot() => null;

    /// <summary>
    /// Rebuild state from a payload previously produced by <see cref="TakeSnapshot"/>.
    /// <paramref name="version"/> is the version that payload covers; the framework then
    /// replays only the events after it through <see cref="Mutate"/>.
    /// <para>
    /// <b>Override this TOGETHER WITH <see cref="TakeSnapshot"/>, or neither.</b> The base
    /// implementation refuses with <see cref="NotSupportedException"/> on purpose: a payload
    /// nothing can restore used to be "restored" silently by an empty implementation, leaving the
    /// state WITHOUT the prefix the payload covers — a wrong aggregate that reports success, the
    /// worst possible failure mode for a recovery accelerator. Because the framework catches the
    /// exception, the only consequence of breaking the pair is that recovery replays the whole
    /// stream (correct, just slower).
    /// </para>
    /// <para>
    /// Read models and other O(n) effects must be skipped here only if replay already skips
    /// them — see <see cref="IsReplaying"/>. Throwing is allowed and safe: recovery records a
    /// WARNING in the runtime's log and falls back to replaying the whole stream, so a corrupt
    /// or stale payload can never break the aggregate or its history.
    /// </para>
    /// <para>
    /// <see cref="OnSnapshotFailed"/> is NOT on this path. It reports the failures of
    /// PRODUCING a snapshot (<see cref="TakeSnapshot"/>) and of WRITING one (the store's save),
    /// both of which happen on a live actor's command turn. A snapshot that cannot be READ or
    /// APPLIED during recovery is only a lost accelerator, so the runtime downgrades it to a
    /// warning and replays everything — there is no caller to notify.
    /// </para>
    /// <para>
    /// CONTRACT for the fallback: validate the payload BEFORE writing any state, and let the
    /// restore be a pure "set fields from payload". A restore that mutates and then throws
    /// leaves residue that the full replay does not roll back — it only re-applies events, so
    /// anything set here that no event rebuilds would survive into the recovered instance.
    /// </para>
    /// </summary>
    protected virtual void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload) =>
        throw new NotSupportedException(
            $"{GetType().Name} overrides TakeSnapshot without RestoreSnapshot: a payload that "
                + "cannot be restored would silently drop the state it covers, so the runtime "
                + "refuses it and replays the whole event stream instead. Override both members, "
                + "or neither."
        );

    /// <summary>
    /// Report that PRODUCING or WRITING a snapshot failed (<see cref="TakeSnapshot"/> threw, or
    /// the store's save threw). Default: silent — snapshotting is an accelerator, and its
    /// failures are never allowed to affect the actor. A throwing override is swallowed as well.
    /// <para>
    /// Recovery is deliberately NOT reported here: a snapshot that cannot be read or applied
    /// falls back to a full replay with a warning in the runtime's log (see
    /// <see cref="RestoreSnapshot"/>).
    /// </para>
    /// </summary>
    protected virtual void OnSnapshotFailed(Exception ex) { }

    /// <summary>
    /// Flush uncommitted events produced during construction.
    /// Called by the consumption loop after SignalReady, before the first mailbox message.
    /// </summary>
    protected override async ValueTask OnReadyAsync()
    {
        await FlushEventsAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Process a mailbox message: dispatch to subclasses, then persist and mutate.
    /// Overrides Actor.ProcessAsync to insert the Persist→Mutate step.
    /// </summary>
    protected override async ValueTask ProcessAsync(Envelope envelope)
    {
        // Snapshot BEFORE dispatch: a throwing handler must not leak the events it
        // already raised into the next command's flush. Those events were never
        // persisted, so keeping them would append a failed command's events as part
        // of the next successful batch (stream corruption) and inflate Version.
        var eventCount = _events.Count;
        // Version is snapshotted too: the count alone is not a reliable rollback baseline
        // (a handler can call CommitEvents() mid-dispatch, which empties the list while
        // Version stays inflated). Restoring the snapshot is exact in every case.
        var versionSnapshot = Version;

        // 1. Dispatch to subclass — RaiseEvent records events without mutating state
        object? result;
        try
        {
            result = await OnMessageAsync(envelope.Command).ConfigureAwait(false);
        }
        catch
        {
            // Only the dispatch is rolled back here — FlushEventsAsync owns its own
            // rollback (doubling it would underflow Version).
            RollbackDispatchEvents(eventCount, versionSnapshot);
            throw;
        }

        // 2. Persist → Mutate → Commit
        await FlushEventsAsync().ConfigureAwait(false);

        // 3. Reply only after successful persistence
        envelope.Tcs?.TrySetResult(result);
    }

    /// <summary>
    /// Drop the events raised by a dispatch that threw (the batch was never persisted) and
    /// restore <c>Version</c> to <paramref name="versionSnapshot"/> — the value captured
    /// before the dispatch. Restoring (instead of subtracting a count) is exact even when
    /// the handler cleared its own staged events with
    /// <see cref="IEventSourcedActor.CommitEvents"/> before throwing: the list is then empty
    /// while the Version is already inflated, and a count-based rollback would leave the
    /// phantom baseline behind for the next flush (permanent ConcurrencyException).
    /// <para>
    /// HANDLERS MUST NOT CALL <see cref="FlushEventsAsync"/> FROM INSIDE A DISPATCH. The
    /// rollback restores <c>Version</c> to the value captured before the dispatch, so an
    /// inner flush that SUCCEEDED (events persisted, Version advanced) followed by a throw
    /// out of the handler would push <c>Version</c> back BELOW the freshly persisted
    /// baseline. The next flush would then compute <c>expectedVersion = Version - count</c>
    /// from that regressed value and either re-append events the store already holds or die
    /// with ConcurrencyException, permanently. Persist after the dispatch returns, or
    /// return normally so the outer flush runs.
    /// </para>
    /// </summary>
    private void RollbackDispatchEvents(int eventCount, ulong versionSnapshot)
    {
        var added = _events.Count - eventCount;
        if (added > 0)
            _events.RemoveRange(eventCount, added);

        Version = versionSnapshot;
    }

    /// <summary>
    /// Persist uncommitted events (if any store is configured), then Mutate each event,
    /// then clear the uncommitted list. No-op if there are no uncommitted events.
    /// Protected so subclasses (e.g., <see cref="SagaActor"/>) can flush after
    /// resuming from an interrupted step.
    /// </summary>
    protected virtual async ValueTask FlushEventsAsync()
    {
        if (_events.Count == 0)
            return;

        // Captured before the list is cleared: the snapshot trigger counts the events this
        // flush actually persisted (batch size, not a fixed 1).
        var flushedEvents = _events.Count;

        // Persist first — if this fails, state remains unchanged (RaiseEvent didn't Mutate).
        // On failure, discard the uncommitted events and roll Version back to the
        // last persisted version. Otherwise Version stays inflated and the failed
        // events leak into the next append, permanently poisoning the actor:
        // expectedVersion = Version - _events.Count would recompute to a stale
        // baseline forever, and a later successful append would reapply discarded
        // commands whose callers already saw an exception.
        if (_eventStore is not null)
        {
            var expectedVersion = Version - (ulong)_events.Count;
            try
            {
                await _eventStore.AppendAsync(Id, expectedVersion, _events).ConfigureAwait(false);
            }
            catch
            {
                RollbackUncommitted();
                throw;
            }
        }

        // Persistence succeeded (or no store) — now safe to mutate state.
        // Mutate is a pure function that must not throw. If it does, the events are
        // already durable but in-memory state can no longer be trusted: fail loud by
        // stopping the actor (registry removal + stop signal). Keeping it alive would
        // retry the leaked, un-mutated batch on the next flush and fail every later
        // command with a misleading ConcurrencyException.
        try
        {
            foreach (var e in _events)
            {
                if (!TryHandleFrameworkEvent(e))
                    Mutate(e);
            }
        }
        catch
        {
            StopAfterMutateFailure();
            throw;
        }

        // Publish AFTER state is consistent. Replay never reaches this path
        // (ReplayEvents bypasses FlushEventsAsync), so recovery is silent by
        // construction. Failures are isolated — events are already durable.
        // (_events.Count > 0 is guaranteed here — empty list early-returned above.)
        if (_publisher is not null)
        {
            var actorId = Id;
            var version = Version;
            var toPublish = _events.ToList();
            try
            {
                await _publisher.PublishAsync(actorId, version, toPublish).ConfigureAwait(false);
            }
            catch
            {
                // Contract: implementations must not throw; this is defense in
                // depth only — never let a publisher failure break the actor.
            }
        }

        ClearEvents();

        // Success path only: a failed flush rolled the batch back and never reached this
        // line, so a snapshot can never describe events that are not in the stream.
        await SaveSnapshotIfDueAsync(flushedEvents).ConfigureAwait(false);
    }

    /// <summary>
    /// Counting snapshot trigger. After a SUCCESSFUL flush, accumulate the batch size and, once
    /// the configured threshold is reached, ask the subclass for a payload and hand it to the
    /// store.
    /// <para>
    /// Never affects the actor: a null payload means "this type does not snapshot", every
    /// failure (payload production or store write) is reported through
    /// <see cref="OnSnapshotFailed"/> and swallowed, and the counter is reset on every attempt
    /// so a failing store is retried at the NEXT threshold instead of on every flush.
    /// </para>
    /// </summary>
    private async ValueTask SaveSnapshotIfDueAsync(int flushedEvents)
    {
        if (_snapshotEveryNEvents is not { } threshold)
            return;

        // Checked before counting: a store that cannot persist snapshots makes the feature a
        // no-op, so the counter must not spin on a capability the store lacks.
        if (_eventStore is not IEventStoreSnapshot snapshotStore)
            return;

        _eventsSinceSnapshot += flushedEvents;
        if (_eventsSinceSnapshot < threshold)
            return;

        // This threshold is consumed whether or not the attempt below succeeds.
        _eventsSinceSnapshot = 0;

        byte[]? payload;
        try
        {
            payload = TakeSnapshot();
        }
        catch (Exception ex)
        {
            ReportSnapshotFailure(ex);
            return;
        }

        if (payload is null)
            return; // The type opts out of snapshotting.

        // A separate failure scope from the TakeSnapshot try above (that one has closed): the tag
        // read and its encoding are subclass code, and an exception here must not escape as a
        // failed AskAsync — the command's batch is already durable and already applied, so a
        // caller-visible failure would make it retry a command that succeeded.
        try
        {
            var schema = SnapshotSchema;
            if (schema is not null)
                payload = WrapSnapshot(schema, payload); // non-null here (checked above)
        }
        catch (Exception ex)
        {
            ReportSnapshotFailure(ex);
            return;
        }

        try
        {
            // The write observes the actor's stop token like any other I/O of this actor: a
            // shutdown interrupt cancels it instead of being held up by the store. Cancellation
            // is reported exactly like a store failure — the batch is already durable, so it can
            // never fail the command that triggered it.
            await snapshotStore
                .SaveSnapshotAsync(Id, new ActorSnapshot(Version, payload), StopToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ReportSnapshotFailure(ex);
        }
    }

    /// <summary>
    /// Deliver a snapshotting failure to <see cref="OnSnapshotFailed"/> without letting a
    /// throwing override escape: snapshotting is an accelerator, and its diagnostics must
    /// never turn a durable, successfully flushed batch into a failed command.
    /// </summary>
    private void ReportSnapshotFailure(Exception ex)
    {
        try
        {
            OnSnapshotFailed(ex);
        }
        catch
        {
            // Diagnostics only.
        }
    }

    /// <summary>
    /// Fatal Mutate failure: the batch is durable but state is not. Stop the actor so
    /// no later command can observe half-applied state or retry the leaked batch.
    /// <see cref="IActorSystem.RequestStop"/> also removes the registry entry, so later
    /// messages fail with <see cref="KeyNotFoundException"/> instead of a misleading
    /// ConcurrencyException.
    /// </summary>
    private void StopAfterMutateFailure()
    {
        if (System is not null)
            System.RequestStop(Id);
        else
            SignalStop();
    }

    /// <summary>
    /// Discard uncommitted events and roll Version back to the last persisted
    /// version. Used when persistence fails (<see cref="FlushEventsAsync"/>) or a
    /// business failure makes the current batch non-atomic (<c>SagaActor.FailAsync</c>):
    /// the next append must compute expectedVersion from the last persisted baseline,
    /// otherwise the discarded events would leak into the next batch permanently.
    /// The lower bound matches <see cref="RollbackDispatchEvents"/>: an inconsistent
    /// Version must never underflow into a huge expectedVersion.
    /// </summary>
    protected void RollbackUncommitted()
    {
        var delta = (ulong)_events.Count;
        Version = Version >= delta ? Version - delta : 0UL;
        ClearEvents();
    }

    private void ClearEvents() => _events.Clear();

    /// <summary>
    /// Initialization is done: the instance is live, so the replay marker's job is
    /// over. Subclass overrides must call the base implementation.
    /// </summary>
    internal override void OnInitialized()
    {
        IsReplaying = false;
        base.OnInitialized();
    }

    void IEventSourcedActor.CommitEvents() => ClearEvents();

    void IEventSourcedActor.ReplayEvents(IReadOnlyList<IDomainEvent> events) =>
        // ReplayEventsFrom(0, ...) IS the historical full replay: same clear, same Mutate
        // loop, Version = events.Count.
        ReplayEventsFrom(0, events);

    void ISnapshotableActor.RestoreSnapshot(ulong version, byte[] payload) =>
        RestoreSnapshot(version, payload.AsSpan());

    void ISnapshotableActor.ReplayEventsFrom(
        ulong baseVersion,
        IReadOnlyList<IDomainEvent> events
    ) => ReplayEventsFrom(baseVersion, events);

    /// <summary>
    /// Replay <paramref name="events"/> on top of <paramref name="baseVersion"/> (0 = the
    /// full stream). Snapshot-accelerated recovery restores the payload first and passes the
    /// snapshot's version here, so only the events after it reach <see cref="Mutate"/>.
    /// </summary>
    private void ReplayEventsFrom(ulong baseVersion, IReadOnlyList<IDomainEvent> events)
    {
        // One marker for both paths: everything replayed is a recovery, including the tail
        // of a snapshot restore, so subclasses skip their O(n) per-event effects either way.
        IsReplaying = true;

        // A rebuilt instance must drop whatever its constructor staged: the persisted
        // stream is authoritative. Keeping those events (a constructor-seeded saga is the
        // real case) would append them again and make every later flush compute
        // expectedVersion from a phantom baseline — a permanent ConcurrencyException.
        ClearEvents();

        // Replay bypasses persistence — events are already in the store.
        // Mutate directly, then set Version.
        foreach (var e in events)
        {
            if (!TryHandleFrameworkEvent(e))
                Mutate(e);
        }
        Version = baseVersion + (ulong)events.Count;
    }
}
