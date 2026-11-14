# Snapshot Schema Envelope Implementation Plan (rev 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give snapshot payloads a state-shape tag so recovery never applies bytes written by a different shape.

**Architecture:** The framework wraps an actor's snapshot payload in a `PAS1` envelope (`magic + 1-byte UTF-8 length + schema + payload`) and validates the tag on restore. Rejection throws from `ISnapshotableActor.RestoreSnapshot`, which the existing `ActorSystem.RestoreOrReplayAsync` catch turns into a full replay. `ActorSnapshot`, `IEventStoreSnapshot` and all stores stay unchanged.

**Tech Stack:** C# / net10.0, TUnit, PicoActor's existing snapshot hooks.

**Spec:** `docs/superpowers/specs/2026-11-14-snapshot-schema-envelope-design.md` (rev 3)

## Global Constraints

- Target `net10.0`. **Code-side touched files:** `src/PicoActor.Abs/EventSourcedActor.cs`, `src/PicoActor.Abs/GlobalUsings.cs` (add `global using System.Text;`), new tests. `api/PicoActor.Abs.public.txt` is **never edited or committed by this plan** (release.ps1 refreshes it at release time). Documentation/release files are touched **only in Task 6**.
- Never modify `ActorSystem.cs`, `ActorSnapshot`, `IEventStoreSnapshot`, `InMemoryEventStore`.
- History is never truncated; one snapshot per aggregate; trigger stays event-count based.
- Only `null` disables the envelope; `""` is a real tag (`schemaLen = 0`). Both sides test `is null`, never `IsNullOrEmpty`.
- Compare tags with `StringComparison.Ordinal`; decode with the non-throwing `Encoding.UTF8` default.
- Detection order: `<5` or magic mismatch → no envelope (`actorPayload = payload`, the whole span); magic + truncated → throw the exact truncation message; else decode + compare.
- Write: new array, never mutate the buffer `TakeSnapshot` returned. Read: zero-copy slice, never mutate the store's payload.
- No snapshot failure may escape `SaveSnapshotIfDueAsync`: `TakeSnapshot`, `SnapshotSchema`, encoding and the store write all end in `ReportSnapshotFailure` + return.
- Build 0 warnings; `dotnet csharpier format` the touched files before each commit; `PicoActor.Tests` = **197** (183 + 14 new), `PicoActor.Abs.Tests` = **51**.
- Helpers stay `private static` in `EventSourcedActor.cs` (the spec's decision; no new file).

## Review Focus

Each line names a reasonable-user expectation and the test that pins it:

1. A legacy raw payload whose first four bytes are `PAS1` → rejected, not half-parsed — Task 3, `LegacyPayloadStartingWithMagic_IsRejected`.
2. An actor whose tag goes from a value to `null` → envelope rejected, full replay, then self-heal with untagged snapshots — Task 3, `Envelope_WithNullSchema_IsRejected`.
3. An empty tag `""` → a real tag (`schemaLen = 0`), written and applied — Task 2, `EmptyTag_IsARealTag_WrittenWithZeroLength`.
4. A `SnapshotSchema` override that throws → command still succeeds, no snapshot, `OnSnapshotFailed` fired — Task 4, `ThrowingSnapshotSchema_DoesNotFailTheCommand`.
5. A tag longer than 255 UTF-8 bytes (100 × `汉` = 300 B) → snapshot skipped once, command succeeds — Task 4, `OversizedTag_SkipsSnapshot_ReportsOnce_CommandSucceeds`.

Documented, not testable in-repo: an older binary reading a new envelope (spec §Residual risks: two-deploy rollout).

## Test fixture (shared by every task)

`tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs` — all of this lives **inside the test class** of that file:

```csharp
internal sealed record SchemaCreate(int Start) : ICommand;
internal sealed record SchemaAdd(int Delta) : ICommand;
internal sealed record SchemaGet : ICommand;
internal sealed record SchemaValueSet(int Value) : IDomainEvent;
internal sealed record SchemaValueAdded(int Delta) : IDomainEvent;

internal sealed class SchemaCounter : EventSourcedActor
{
    private int _value;
    public string? Tag { get; init; }                    // null = no envelope
    public SnapshotFailureSink? Failures { get; init; }
    public bool ThrowOnSchema { get; init; }
    public bool ThrowOnTake { get; init; }
    public int MutateCalls { get; private set; }

    public SchemaCounter(SchemaCreate cmd) : base(cmd) { }
    public SchemaCounter() { }

    protected override string? SnapshotSchema =>
        ThrowOnSchema ? throw new InvalidOperationException("schema boom") : Tag;

    protected override ValueTask<object?> OnMessageAsync(ICommand command)
    {
        switch (command)
        {
            case SchemaCreate c: RaiseEvent(new SchemaValueSet(c.Start)); return default;
            case SchemaAdd a:    RaiseEvent(new SchemaValueAdded(a.Delta)); return default;
            case SchemaGet:      return new ValueTask<object?>(_value);
            default:             return default;
        }
    }

    protected override void Mutate(IDomainEvent e)
    {
        MutateCalls++;
        if (e is SchemaValueSet s) _value = s.Value;
        else if (e is SchemaValueAdded a) _value += a.Delta;
    }

    protected override void OnSnapshotFailed(Exception ex) => Failures?.Add(ex);

    protected override byte[]? TakeSnapshot() =>
        ThrowOnTake ? throw new InvalidOperationException("take boom") : BitConverter.GetBytes(_value);

    protected override void RestoreSnapshot(ulong version, ReadOnlySpan<byte> payload) =>
        _value = BitConverter.ToInt32(payload);
}
```

Helpers (inside the same test class):

```csharp
private static ActorSystem NewSystem(InMemoryEventStore store, int? snapshotEvery = null) =>
    new(new ActorSystemOptions { EventStore = store, SnapshotEveryNEvents = snapshotEvery });

private static void RegisterSameTag(ActorSystem system, string? tag,
    SnapshotFailureSink? failures = null, bool throwOnSchema = false, bool throwOnTake = false) =>
    system.Register<SchemaCounter>(
        c => new SchemaCounter((SchemaCreate)c) { Tag = tag, Failures = failures, ThrowOnSchema = throwOnSchema, ThrowOnTake = throwOnTake },
        () => new SchemaCounter { Tag = tag, Failures = failures, ThrowOnSchema = throwOnSchema, ThrowOnTake = throwOnTake });

// literal rebuild tag: null here really means null, never falls back to the create tag
private static void RegisterTagChange(ActorSystem system, string? createTag, string? rebuildTag,
    SnapshotFailureSink? failures = null) =>
    system.Register<SchemaCounter>(
        c => new SchemaCounter((SchemaCreate)c) { Tag = createTag, Failures = failures },
        () => new SchemaCounter { Tag = rebuildTag, Failures = failures });
```

Reuse `SnapshotFailureSink` (internal, `FixSSnapshotTests.cs`) — it is in this assembly.

---

### Task 1: `SnapshotSchema` hook + envelope writing

**Files:**
- Modify: `src/PicoActor.Abs/EventSourcedActor.cs` (hook near `TakeSnapshot`; write path in `SaveSnapshotIfDueAsync`)
- Modify: `src/PicoActor.Abs/GlobalUsings.cs`
- Test: `tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs`

**Interfaces:**
- Consumes: `TakeSnapshot()`, `ReportSnapshotFailure(Exception)`, the existing save `try` block.
- Produces: `protected virtual string? SnapshotSchema` (default `null`); `private static byte[] WrapSnapshot(string schema, byte[] payload)`; `private static ReadOnlySpan<byte> SnapshotMagic => "PAS1"u8`.

- [ ] **Step 1: Write the failing tests**

```csharp
[Test, Timeout(15000)]
public async Task Envelope_IsWritten_WithUtf8ByteLength()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "订单-v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 4; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // 5 events -> v5, value 4

    var snap = await store.LoadSnapshotAsync(actor.Id);
    await Assert.That(snap).IsNotNull();
    var tag = "订单-v1";
    var tagBytes = System.Text.Encoding.UTF8.GetBytes(tag);
    await Assert.That(snap!.Payload.Length).IsEqualTo(5 + tagBytes.Length + 4);
    await Assert.That(snap.Payload[0..4]).IsEquivalentTo("PAS1"u8.ToArray());
    await Assert.That(snap.Payload[4]).IsEqualTo((byte)tagBytes.Length);        // 9 bytes; do not hard-code
    await Assert.That(snap.Payload[5..(5 + tagBytes.Length)]).IsEquivalentTo(tagBytes);
    await Assert.That(BitConverter.ToInt32(snap.Payload, 5 + tagBytes.Length)).IsEqualTo(4);
}

[Test, Timeout(15000)]
public async Task NullSchema_WritesRawPayload_Unchanged()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, null);
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 4; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    var snap = await store.LoadSnapshotAsync(actor.Id);
    await Assert.That(snap!.Payload.Length).IsEqualTo(4);
    await Assert.That(BitConverter.ToInt32(snap.Payload)).IsEqualTo(4);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj --no-progress -- --treenode-filter "/*/*/FixTSnapshotSchemaTests/*"`
Expected: compile errors — `'SchemaCounter' does not override 'SnapshotSchema'` / `SnapshotSchema` not found.

- [ ] **Step 3: Implement the hook and the write-side envelope**

Add `protected virtual string? SnapshotSchema => null;` with an XML doc covering: bump when the
state shape changes (old snapshots become invalid once); only `null` disables it; `""` is a real
tag; **the tag must be type-level (both the create and rebuild factories must set it
identically — a mismatch silently turns every recovery into a full replay)**; and
`ISnapshotableActor` implementations outside `EventSourcedActor` must unwrap by hand.

In `SaveSnapshotIfDueAsync`, **after** the existing `if (payload is null) return;`, open a **new**
failure scope (the existing `TakeSnapshot` try has already closed):

```csharp
try
{
    var schema = SnapshotSchema;
    if (schema is not null)
        payload = WrapSnapshot(schema, payload);   // payload is non-null here (checked above)
}
catch (Exception ex)
{
    ReportSnapshotFailure(ex);
    return;
}
```

`WrapSnapshot`: `[PAS1][len][schema][payload]`; `len = Encoding.UTF8.GetByteCount(schema)`; no
`>255` check yet (Task 4).

- [ ] **Step 4: Run to verify pass** — same filter, expected PASS (2/2).

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor.Abs/EventSourcedActor.cs src/PicoActor.Abs/GlobalUsings.cs tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs
git add src/PicoActor.Abs/EventSourcedActor.cs src/PicoActor.Abs/GlobalUsings.cs tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs
git commit -m "feat(abs): write a PAS1 schema envelope on snapshots"
```

---

### Task 2: Unwrap + the accept path (no rejection yet, truncation included)

**Files:** Modify `EventSourcedActor.cs` (explicit `ISnapshotableActor.RestoreSnapshot`, `TryUnwrapSnapshot`); Test `FixTSnapshotSchemaTests.cs`.

**Interfaces:**
- Consumes: Task 1's `WrapSnapshot`/`SnapshotMagic`; protected `RestoreSnapshot(ulong, ReadOnlySpan<byte>)`.
- Produces: `private static bool TryUnwrapSnapshot(ReadOnlySpan<byte> payload, out ReadOnlySpan<byte> actorPayload, out string? schema)` — `true` only when an envelope is present; otherwise `false`, `actorPayload = payload` (whole span), `schema = null`.

- [ ] **Step 1: Write the failing tests**

```csharp
[Test, Timeout(15000)]
public async Task Envelope_IsApplied_ReplayingOnlyTheTail()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // 12 events, snapshot at v10 (value 9)
    await system.StopAsync(actor.Id);
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt).IsNotNull();
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}

[Test, Timeout(15000)]
public async Task LegacyRawPayload_WithNullSchema_IsApplied()   // regression pin: green on base
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, null);
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await system.StopAsync(actor.Id);
    // v10's state value is 9 (value = version - 1); overwrite with a raw (unenveloped) payload
    await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, BitConverter.GetBytes(9)));
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}

[Test, Timeout(15000)]
public async Task EmptyTag_IsARealTag_WrittenWithZeroLength()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    var snap = await store.LoadSnapshotAsync(actor.Id);
    await Assert.That(snap!.Payload.Length).IsEqualTo(5 + 4);
    await Assert.That(snap.Payload[4]).IsEqualTo((byte)0);
    await system.StopAsync(actor.Id);
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(2);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}

[Test, Timeout(15000)]
public async Task Envelope_IsRestoredTwiceFromTheSameStoredSnapshot()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // snapshot at v10
    await system.StopAsync(actor.Id);
    var before = (await store.LoadSnapshotAsync(actor.Id))!.Payload.ToArray();  // copy: the store returns the same instance

    var first = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(first!.MutateCalls).IsEqualTo(2);
    await Assert.That(await system.AskAsync<int>(first.Id, new SchemaGet())).IsEqualTo(11);
    await system.StopAsync(actor.Id);
    var second = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(second!.MutateCalls).IsEqualTo(2);
    await Assert.That(await system.AskAsync<int>(second.Id, new SchemaGet())).IsEqualTo(11);
    await Assert.That((await store.LoadSnapshotAsync(actor.Id))!.Payload.SequenceEqual(before)).IsTrue();
}

[Test, Timeout(15000)]
public async Task TruncatedEnvelope_IsRejected_WithInvalidOperationException()
{
    // deterministic: call the public interface directly on an unattached instance
    var probe = new SchemaCounter();
    var truncated = new byte[] { 0x50, 0x41, 0x53, 0x31, 0x09, 0x01, 0x02 }; // magic + schemaLen 9, 2 bytes left
    var ex = Assert.Throws<InvalidOperationException>(
        () => ((ISnapshotableActor)probe).RestoreSnapshot(10, truncated));
    await Assert.That(ex.Message).Contains("truncated");

    // end-to-end: the stored truncated envelope degrades to a full replay
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await system.StopAsync(actor.Id);
    await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, truncated));
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}
```

- [ ] **Step 2: Run to verify failure**

Expected: 4 FAIL (`Envelope_IsApplied…`, `EmptyTag…`, `Envelope_IsRestoredTwice…`,
`TruncatedEnvelope…` — the envelope bytes reach `RestoreSnapshot` as-is, so the value is wrong and
the direct call throws and the stored payload misreads) and 1 PASS (`LegacyRawPayload_WithNullSchema_IsApplied` is a
backward-compat pin, green on base).

- [ ] **Step 3: Implement `TryUnwrapSnapshot` and the accept-only explicit interface method**

```csharp
void ISnapshotableActor.RestoreSnapshot(ulong version, byte[] payload)
{
    if (TryUnwrapSnapshot(payload, out var actorPayload, out _))
    {
        RestoreSnapshot(version, actorPayload);   // slice, zero-copy
        return;
    }
    RestoreSnapshot(version, payload);            // no envelope: the whole span
}
```

`TryUnwrapSnapshot`: `<5` or magic mismatch → `false`, `actorPayload = payload`, `schema = null`;
magic + `payload.Length < 5 + payload[4]` → `throw new InvalidOperationException(
$"snapshot envelope is truncated: schemaLen={payload[4]}, payload={payload.Length}")`; else decode
`payload[5..5+len]` with `Encoding.UTF8.GetString` (non-throwing),
`actorPayload = payload[(5 + len)..]`, return `true`.

- [ ] **Step 4: Run to verify pass** — 5/5 PASS.

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor.Abs/EventSourcedActor.cs tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs
git commit -am "feat(abs): unwrap the PAS1 snapshot envelope on restore"
```

---

### Task 3: Rejection matrix (tag change, legacy payloads, magic collision)

**Files:** Modify `EventSourcedActor.cs` (add the two throws); Test `FixTSnapshotSchemaTests.cs`.

**Interfaces:**
- Consumes: Task 2's `TryUnwrapSnapshot`; `SnapshotSchema`.
- Produces: no new surface — the explicit `RestoreSnapshot` now throws on mismatch and on an untagged payload when a tag is declared.

- [ ] **Step 1: Write the failing tests**

```csharp
[Test, Timeout(15000)]
public async Task ChangedTag_IsRejected_AndFullReplay_ThenSelfHealsAtNextThreshold()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterTagChange(system, createTag: "v1", rebuildTag: "v2");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // v10 snapshot, value 9
    await system.StopAsync(actor.Id);
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);                                 // full replay
    for (var i = 0; i < 5; i++) await system.AskAsync<object?>(rebuilt.Id, new SchemaAdd(1)); // v17: one more snapshot
    var snap = await store.LoadSnapshotAsync(rebuilt.Id);
    var tagBytes = System.Text.Encoding.UTF8.GetBytes("v2");
    await Assert.That(snap!.Payload[0..4]).IsEquivalentTo("PAS1"u8.ToArray());
    await Assert.That(snap.Payload[5..(5 + tagBytes.Length)]).IsEquivalentTo(tagBytes);
}

[Test, Timeout(15000)]
public async Task LegacyRawPayload_WithDeclaredSchema_IsRejected_FullReplay()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await system.StopAsync(actor.Id);
    // a VALID raw payload the actor would accept: v10 = 9
    await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, BitConverter.GetBytes(9)));
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}

[Test, Timeout(15000)]
public async Task Envelope_WithNullSchema_IsRejected()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterTagChange(system, createTag: "v1", rebuildTag: null);
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await system.StopAsync(actor.Id);
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);
    for (var i = 0; i < 5; i++) await system.AskAsync<object?>(rebuilt.Id, new SchemaAdd(1));
    var snap = await store.LoadSnapshotAsync(rebuilt.Id);
    await Assert.That(snap!.Payload[0..4].SequenceEqual("PAS1"u8.ToArray())).IsFalse(); // self-heal: raw again
}

[Test, Timeout(15000)]
public async Task LegacyPayloadStartingWithMagic_IsRejected()
{
    var store = new InMemoryEventStore();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1");
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await system.StopAsync(actor.Id);
    var payload = new byte[] { 0x50, 0x41, 0x53, 0x31, 0x03, (byte)'a', (byte)'b', (byte)'c', 9, 0, 0, 0 };
    await store.SaveSnapshotAsync(actor.Id, new ActorSnapshot(10, payload));
    var rebuilt = await system.GetAsync<SchemaCounter>(actor.Id);
    await Assert.That(rebuilt!.MutateCalls).IsEqualTo(12);
    await Assert.That(await system.AskAsync<int>(rebuilt.Id, new SchemaGet())).IsEqualTo(11);
}
```

- [ ] **Step 2: Run to verify failure** — expected: all 4 FAIL (the accept-only path applies the bytes instead of rejecting).

- [ ] **Step 3: Implement the two throws**

```csharp
void ISnapshotableActor.RestoreSnapshot(ulong version, byte[] payload)
{
    if (!TryUnwrapSnapshot(payload, out var actorPayload, out var stored))
    {
        var current = SnapshotSchema;
        if (current is not null)
            throw new InvalidOperationException(
                $"unversioned snapshot ignored because the actor declares schema '{current}'.");
        RestoreSnapshot(version, actorPayload);
        return;
    }
    if (!string.Equals(stored, SnapshotSchema, StringComparison.Ordinal))
        throw new InvalidOperationException(
            $"snapshot schema mismatch: stored '{stored}' vs current '{SnapshotSchema ?? "<null>"}'.");
    RestoreSnapshot(version, actorPayload);
}
```

(The truncation throw already exists in `TryUnwrapSnapshot` from Task 2.)

- [ ] **Step 4: Run to verify pass** — 4/4 PASS.

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor.Abs/EventSourcedActor.cs tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs
git commit -am "feat(abs): reject mismatched or unversioned snapshot payloads"
```

---

### Task 4: Failure paths (oversized tag is the only new behaviour)

**Files:** Modify `EventSourcedActor.cs` (add the `>255` check); Test `FixTSnapshotSchemaTests.cs`.

**Interfaces:**
- Consumes: `SnapshotSchema`, `WrapSnapshot(string, byte[])`, `ReportSnapshotFailure(Exception)` (Task 1's write scope).
- Produces: no new surface — `SaveSnapshotIfDueAsync` skips oversized tags.

- [ ] **Step 1: Write the failing tests**

```csharp
[Test, Timeout(15000)]
public async Task OversizedTag_SkipsSnapshot_ReportsOnce_CommandSucceeds()
{
    var store = new InMemoryEventStore();
    var failures = new SnapshotFailureSink();
    var system = NewSystem(store, snapshotEvery: 12);
    RegisterSameTag(system, new string('汉', 100), failures);   // 300 UTF-8 bytes
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 11; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // 12 events = one trigger
    await Assert.That(await store.LoadSnapshotAsync(actor.Id)).IsNull();
    await Assert.That(failures.Failures.Count).IsEqualTo(1);
    await Assert.That(await system.AskAsync<int>(actor.Id, new SchemaGet())).IsEqualTo(11); // still usable
}

[Test, Timeout(15000)]
public async Task ThrowingSnapshotSchema_DoesNotFailTheCommand()   // regression pin: green since Task 1
{
    var store = new InMemoryEventStore();
    var failures = new SnapshotFailureSink();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, "v1", failures, throwOnSchema: true);
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0)); // must not throw
    for (var i = 0; i < 4; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1)); // must not throw
    await Assert.That(await store.LoadSnapshotAsync(actor.Id)).IsNull();
    await Assert.That(failures.Failures.Count).IsEqualTo(1);
}

[Test, Timeout(15000)]
public async Task ThrowingTakeSnapshot_DoesNotFailTheCommand()     // regression pin: green on base
{
    var store = new InMemoryEventStore();
    var failures = new SnapshotFailureSink();
    var system = NewSystem(store, snapshotEvery: 5);
    RegisterSameTag(system, null, failures, throwOnTake: true);
    var actor = await system.CreateAsync<SchemaCounter>(new SchemaCreate(0));
    for (var i = 0; i < 4; i++) await system.AskAsync<object?>(actor.Id, new SchemaAdd(1));
    await Assert.That(await store.LoadSnapshotAsync(actor.Id)).IsNull();
    await Assert.That(failures.Failures.Count).IsEqualTo(1);
    await Assert.That(await system.AskAsync<int>(actor.Id, new SchemaGet())).IsEqualTo(4);
}
```

- [ ] **Step 2: Run to verify failure** — expected: only `OversizedTag…` FAILS (the 300-byte length
  is truncated to a byte and a corrupt envelope is written). The two pins PASS.

- [ ] **Step 3: Implement the `>255` check** — inside Task 1's write scope, after reading the
  schema and before wrapping:

```csharp
var byteCount = Encoding.UTF8.GetByteCount(schema);
if (byteCount > 255)
{
    ReportSnapshotFailure(new InvalidOperationException(
        $"snapshot schema is too long: {byteCount} UTF-8 bytes (max 255)."));
    return;
}
```

- [ ] **Step 4: Run to verify pass** — 3/3 PASS.

- [ ] **Step 5: Format and commit**

```bash
dotnet csharpier format src/PicoActor.Abs/EventSourcedActor.cs tests/PicoActor.Tests/FixTSnapshotSchemaTests.cs
git commit -am "fix(abs): skip oversized snapshot tags without failing the command"
```

---

### Task 5: Verification and mutation checks (no file to commit)

**Files:** none committed. The public API baseline is **not** touched here (release.ps1 refreshes it at release time; pre-refreshing would make `-DryRun` answer “API unchanged” = y bump).

**Interfaces:**
- Consumes: Task 1's `SnapshotSchema` (the only API delta).
- Produces: verification evidence only.

- [ ] **Step 1: Full build and test**

```bash
dotnet build PicoActor.slnx -m:1 -c Release
dotnet test tests/PicoActor.Tests/PicoActor.Tests.csproj -c Release --no-build --no-progress
dotnet test tests/PicoActor.Abs.Tests/PicoActor.Abs.Tests.csproj -c Release --no-build --no-progress
dotnet csharpier check .
```
Expected: 0 warnings; `PicoActor.Tests` = 197; `Abs.Tests` = 51; CSharpier exit 0.

- [ ] **Step 2: Confirm the API delta without touching the baseline**

```bash
dotnet run --project scripts/api-surface -- dump src/PicoActor.Abs/bin/Release/net10.0/PicoActor.Abs.dll --out artifacts/api-current.txt
dotnet run --project scripts/api-surface -- diff api/PicoActor.Abs.public.txt artifacts/api-current.txt
```
Expected: `added=1 removed=0`, the added line is exactly
`P:PicoActor.Abs.EventSourcedActor.SnapshotSchema : string { get; }`. Note: `diff` exits **1** when
surfaces differ — that is the expected outcome here; do not treat the non-zero exit as a failure.

- [ ] **Step 3: Mutation checks (temporary edits, reverted — do not commit)**

For each: edit → run the named test → confirm red → revert.

| Mutation | Test that must go red |
|---|---|
| delete the tag comparison | `ChangedTag_…` |
| treat `""` as `null` | `EmptyTag_IsARealTag_WrittenWithZeroLength` |
| move the schema read outside the failure scope | `ThrowingSnapshotSchema_DoesNotFailTheCommand` |
| mutate the store's array instead of slicing (write `actorPayload` back in place) | `Envelope_IsRestoredTwiceFromTheSameStoredSnapshot` |
| truncate instead of skipping an oversized tag | `OversizedTag_SkipsSnapshot_ReportsOnce_CommandSucceeds` |
| drop the `try` around `TakeSnapshot` | `ThrowingTakeSnapshot_DoesNotFailTheCommand` |

---

### Task 6: Documentation + release

**Files:** `README.md` + 9 translations, `picohex-actor` skill (outside the repo), `docs/release-notes/v2026.7.0.md`.

**Interfaces:** Consumes the final API (`SnapshotSchema`) and the spec's rollout section. Produces no code.

- [ ] **Step 1: README (English first, then mirror all 9 translations)** — in the Snapshot section add 3–4 lines: `SnapshotSchema`; changing it invalidates old snapshots once (self-heal at the next threshold); `null` = no protection; envelope layout. Keep code blocks identical across languages.

- [ ] **Step 2: `picohex-actor` skill** — same content in §Snapshot acceleration, plus one Common Mistakes row (“changed the state shape without bumping `SnapshotSchema`”).

- [ ] **Step 3: Release notes** — `docs/release-notes/v2026.7.0.md` following the existing structure (Why / Content / API / wire / breaking / Limitations left open): x bump, `SnapshotSchema` delta, the two-deploy rollout caveat, the legacy-`PAS1` collision cost, and the oversized-tag/lone-surrogate note.

- [ ] **Step 4: Commit the docs**

```bash
git add README*.md docs/release-notes/v2026.7.0.md
git commit -m "docs: document SnapshotSchema and the envelope rollout"
```

- [ ] **Step 5: Dry run** — from `main`, clean tree: `scripts/release.ps1 -DryRun` must report **API changed → v2026.7.0**. If it says “unchanged”, the baseline was pre-refreshed: stop and report, do not hand-edit `api/`.

- [ ] **Step 6: Apply the release** — run `./scripts/release.ps1` **without** `-DryRun` (there is no `-Apply` parameter; `picohex-release` step 5 is "run without `-DryRun`"). It refreshes `api/` and creates the release commit + tag.

- [ ] **Step 7: Push and watch** — push the branch and tag; watch the Release workflow and CI to completion (tag + Release `published`, `checks: green`). Do not claim “published” until both are observed.

- [ ] **Step 8: Verify the artifact** — download the published `.nupkg`, extract it, and confirm `README.md` contains the new `SnapshotSchema` lines.

## Self-Review

1. **Spec coverage:** format → Task 1/2; detection + matrix → Task 2/3; failures → Task 4; zero-copy/read-only → Task 2 (+ double-rebuild test); rollout → Task 6; docs → Task 6. All 14 tests are placed: Task 1 (1, 7), Task 2 (2, 5, 10, 11, 14), Task 3 (3, 4, 8, 12), Task 4 (6, 9, 13).
2. **Step scan:** every step is one action with a checkable result; test steps carry names + assertions with the spec's values; code steps carry signatures and only the byte-layout bodies; each coding task ends with format + commit.
3. **Type consistency:** `SnapshotSchema` (string?), `WrapSnapshot(string, byte[])`, `TryUnwrapSnapshot(ReadOnlySpan<byte>, out ReadOnlySpan<byte>, out string?)`, `SchemaCounter` members — identical across tasks.
4. **Review Focus:** five lines, each mapped to a named test in a task; the sixth (old binary reading an envelope) is documented as not testable in-repo.
5. **Proportion:** plan ≈ 620 lines, spec ≈ 290 lines; no implementation transcript.
