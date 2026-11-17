using System.Buffers;
using PicoJetson;
using PicoSerDe.Core;

namespace PicoActor.EventStore.Jsonl.Tests;

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
public class EventTypeBindingTests
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
