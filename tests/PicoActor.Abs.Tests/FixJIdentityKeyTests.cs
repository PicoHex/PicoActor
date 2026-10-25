using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using PicoActor.Gen;

namespace PicoActor.Abs.Tests;

// Task J3 (review M-1): the registration identity must include the EVENT type.
//
// A handler may implement IDomainEventSubscriber<T> more than once, for different events —
// one class, one instance factory, but two independent registrations (a container key of
// IDomainEventSubscriber<Paid> and IDomainEventSubscriber<Shipped>). The incremental
// pipeline dedupes on a STRING key, and the key is what keeps those two apart. Drop the
// event component from the key and both visits collapse onto the first one, so the second
// event type is silently never subscribed — with a fully green build.
public sealed class FixJIdentityKeyTests
{
    private const string Source = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;

        namespace Shop
        {
            public record Paid(int Id) : IDomainEvent;

            public record Shipped(int Id) : IDomainEvent;

            public sealed class TwoEventHandler
                : IDomainEventSubscriber<Paid>, IDomainEventSubscriber<Shipped>
            {
                public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;

                public ValueTask Handle(DomainEventEnvelope<Shipped> envelope, ICommandSender sender, CancellationToken ct)
                    => ValueTask.CompletedTask;
            }
        }
        """;

    /// <summary>
    /// How many times the generated configurator registers THIS handler for THIS event type:
    /// one SvcDescriptor.Create whose service key is the event-typed subscriber and whose
    /// factory constructs the handler. Joined with Environment.NewLine because generated code
    /// is emitted through StringBuilder.AppendLine (platform newline).
    /// </summary>
    private static int RegistrationCount(string generated, string eventTypeFqn) =>
        FixCGeneratorHarness.CountOccurrences(
            generated,
            "SvcDescriptor.Create("
                + Environment.NewLine
                + "            typeof(global::PicoActor.Abs.IDomainEventSubscriber<"
                + eventTypeFqn
                + ">),"
                + Environment.NewLine
                + "            static _ => new global::Shop.TwoEventHandler(),"
        );

    [Test]
    public async Task HandlerImplementingTwoEventInterfaces_GetsOneRegistrationPerEvent()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", Source);
        var source = FixCGeneratorHarness.FindGeneratedSource(
            driver,
            FixCGeneratorHarness.HintNamePart
        );

        await Assert.That(source).IsNotNull();

        // Same handler, two factories — one per event type (the key must not collapse them).
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "static _ => new global::Shop.TwoEventHandler()"
                )
            )
            .IsEqualTo(2);

        // …under the two event-typed service keys, each exactly once.
        await Assert.That(RegistrationCount(source!, "global::Shop.Paid")).IsEqualTo(1);
        await Assert.That(RegistrationCount(source!, "global::Shop.Shipped")).IsEqualTo(1);

        // One envelope bridge per distinct event type.
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "internal sealed class PicoActorEnvelopeBridge_"
                )
            )
            .IsEqualTo(2);

        await Assert
            .That(
                driver
                    .GetRunResult()
                    .Diagnostics.Where(static d => d.Severity == DiagnosticSeverity.Error)
            )
            .IsEmpty();
    }
}
