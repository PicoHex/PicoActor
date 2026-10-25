namespace PicoActor.Tests;

// ═══════════════════════════════════════════════════════════
// A2 — a failing creation command must fail CreateAsync. The constructor only
// checked IsCompleted, so a faulted (or canceled) ValueTask counted as "done":
// the exception was swallowed, CreateAsync returned a half-initialized actor, and
// events raised before the throw were flushed as a half-written stream.
// ═══════════════════════════════════════════════════════════

internal sealed record FixACreateThrow : ICommand;

internal sealed record FixACreateThrowAfterRaise : ICommand;

internal sealed record FixACreateSyncThrow : ICommand;

internal sealed record FixACreateCompletesLater : ICommand;

internal sealed record FixACreateOk : ICommand;

internal sealed record FixASeed(int Value) : IDomainEvent;

internal sealed class FixAAsyncCreationActor : EventSourcedActor
{
    public FixAAsyncCreationActor(ICommand creationCommand)
        : base(creationCommand) { }

    public FixAAsyncCreationActor() { }

    protected override async ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case FixACreateThrow:
                // async shape that still completes synchronously → faulted ValueTask
                await Task.CompletedTask.ConfigureAwait(false);
                throw new InvalidOperationException("async creation boom");
            case FixACreateThrowAfterRaise:
                RaiseEvent(new FixASeed(1));
                await Task.CompletedTask.ConfigureAwait(false);
                throw new InvalidOperationException("async creation boom after raise");
            case FixACreateCompletesLater:
                return await CompletesLaterAsync().ConfigureAwait(false);
            case FixACreateOk:
                RaiseEvent(new FixASeed(42));
                return default;
            default:
                return null;
        }
    }

    /// <summary>
    /// A creation handler that NEVER completes. Deterministic by construction: nothing in
    /// this suite completes the gate, so the ValueTask is incomplete at the instant the
    /// constructor checks it — no window, no dependency on how loaded the machine is. The
    /// old <c>await Task.Delay(50)</c> only made it *probable* that the check ran inside
    /// the delay: a machine that stalls the test thread for 50 ms before the constructor
    /// reaches the check (GC pause, debugger, CI contention) would see a COMPLETED ValueTask
    /// and the test would pass/fail on timing instead of on the contract.
    /// <para>
    /// It is NOT leak-free, and the earlier "leaks nothing" wording was wrong: the actor that
    /// awaits this gate is stopped by the failing constructor, but its pending continuation
    /// stays registered on a task that is never completed, so the stopped actor graph —
    /// including this static gate — stays reachable until the test process exits. Bounded and
    /// harmless: one actor per run of this test, in a process that ends when the suite does.
    /// </para>
    /// </summary>
    private static readonly TaskCompletionSource<object?> NeverCompletingGate = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    private static ValueTask<object?> CompletesLaterAsync() => new(NeverCompletingGate.Task);

    protected override void Mutate(IDomainEvent @event) { }
}

/// <summary>The pre-existing synchronous `=> throw` shape — behavior must not change.</summary>
internal sealed class FixASyncCreationThrowActor : Actor
{
    public FixASyncCreationThrowActor(FixACreateSyncThrow cmd)
        : base(cmd) { }

    public FixASyncCreationThrowActor() { }

    protected override ValueTask<object?> OnMessageAsync(ICommand command) =>
        throw new InvalidOperationException("sync creation boom");
}

public sealed class FixACreationFailureTests
{
    private static (ActorSystem System, InMemoryEventStore Store) NewSystem()
    {
        var store = new InMemoryEventStore();
        var system = new ActorSystem(new ActorSystemOptions { EventStore = store });
        system.Register<FixAAsyncCreationActor>(
            cmd => new FixAAsyncCreationActor(cmd),
            () => new FixAAsyncCreationActor()
        );
        system.Register<FixASyncCreationThrowActor>(cmd => new FixASyncCreationThrowActor(
            (FixACreateSyncThrow)cmd
        ));
        return (system, store);
    }

    /// <summary>
    /// A synchronous-throwing async handler faults its ValueTask. CreateAsync must
    /// surface that ORIGINAL exception and must not register anything.
    /// </summary>
    [Test]
    public async Task AsyncCreationHandlerThatThrows_FailsCreateAsync()
    {
        var (system, store) = NewSystem();

        InvalidOperationException? caught = null;
        try
        {
            await system.CreateAsync<FixAAsyncCreationActor>(new FixACreateThrow());
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsNotNull();
        // The original exception, not the "must complete synchronously" guard.
        await Assert.That(caught!.Message).IsEqualTo("async creation boom");

        // The constructor threw before the runtime could assign an identity or add the
        // actor to the registry, so the dead instance is unreachable: nothing answers.
        await Assert
            .That(async () => await system.AskAsync<object?>(Guid.Empty, new FixACreateThrow()))
            .Throws<KeyNotFoundException>();

        var aggregates = await ((IEventStoreEnumerator)store).ListAggregateIdsAsync(
            nameof(FixASeed)
        );
        await Assert.That(aggregates).IsEmpty();

        // The failed creation leaves the type usable: a healthy creation still works
        // and is registered under its own identity.
        var ok = await system.CreateAsync<FixAAsyncCreationActor>(new FixACreateOk());
        await Assert.That(ok.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(await system.AskAsync<object?>(ok.Id, new FixACreateOk())).IsNull();
    }

    /// <summary>
    /// Events raised before the creation handler throws must never be persisted:
    /// the failed construction leaves no half-written stream.
    /// </summary>
    [Test]
    public async Task AsyncCreationHandlerThatRaisesThenThrows_PersistsNothing()
    {
        var (system, store) = NewSystem();

        var threwWithOriginal = false;
        try
        {
            await system.CreateAsync<FixAAsyncCreationActor>(new FixACreateThrowAfterRaise());
        }
        catch (InvalidOperationException ex)
        {
            threwWithOriginal = ex.Message == "async creation boom after raise";
        }

        await Assert.That(threwWithOriginal).IsTrue();

        // No stream was started under any id — a leaked half-batch would show up here.
        var aggregates = await ((IEventStoreEnumerator)store).ListAggregateIdsAsync(
            nameof(FixASeed)
        );
        await Assert.That(aggregates).IsEmpty();

        // The store stays writable and consistent afterwards (no orphaned version).
        var ok = await system.CreateAsync<FixAAsyncCreationActor>(new FixACreateOk());
        var stream = await store.LoadAsync(ok.Id);
        await Assert.That(stream.Count).IsEqualTo(1);
        await Assert.That(ok.Version).IsEqualTo(1UL);
    }

    /// <summary>The old synchronous `=> throw` shape keeps failing CreateAsync.</summary>
    [Test]
    public async Task SyncCreationHandlerThatThrows_StillFailsCreateAsync()
    {
        var (system, _) = NewSystem();

        await Assert
            .That(async () =>
                await system.CreateAsync<FixASyncCreationThrowActor>(new FixACreateSyncThrow())
            )
            .Throws<InvalidOperationException>();
    }

    /// <summary>
    /// A handler that completes later is still a contract violation
    /// (creation commands must be synchronous) → InvalidOperationException.
    /// </summary>
    [Test]
    [Timeout(15000)]
    public async Task CreationHandlerThatCompletesLater_ThrowsInvalidOperation(
        CancellationToken cancellationToken = default
    )
    {
        var (system, _) = NewSystem();

        InvalidOperationException? caught = null;
        try
        {
            await system.CreateAsync<FixAAsyncCreationActor>(new FixACreateCompletesLater());
        }
        catch (InvalidOperationException ex)
        {
            caught = ex;
        }

        await Assert.That(caught).IsNotNull();
        await Assert
            .That(caught!.Message)
            .Contains("must complete synchronously for creation commands");
    }
}
