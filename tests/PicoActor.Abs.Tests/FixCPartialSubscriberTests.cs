using Microsoft.CodeAnalysis;

namespace PicoActor.Abs.Tests;

// A handler whose base list is spread over two partial declarations is visited
// TWICE by the syntax-driven scan, while GetDeclaredSymbol returns the same merged
// symbol both times — so the naive pipeline emits the same registration twice and
// PicoDI registers the handler twice for one event type.
public sealed class FixCPartialSubscriberTests
{
    private const string TwoPartialBaseListsSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;

        public record Paid(int Id) : IDomainEvent;

        public sealed partial class PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }

        public sealed partial class PaidHandler : IDomainEventSubscriber<Paid>
        {
        }
        """;

    [Test]
    public async Task PartialTypeWithBaseListInBothParts_RegistersHandlerOnce()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", TwoPartialBaseListsSource);
        var source = FixCGeneratorHarness.FindGeneratedSource(
            driver,
            FixCGeneratorHarness.HintNamePart
        );

        await Assert.That(source).IsNotNull();
        await Assert
            .That(
                FixCGeneratorHarness.CountOccurrences(
                    source!,
                    "static _ => new global::PaidHandler()"
                )
            )
            .IsEqualTo(1);
    }

    [Test]
    public async Task PartialTypeWithBaseListInBothParts_GeneratesNoDiagnostic()
    {
        var result = FixCGeneratorHarness
            .RunGenerator("App", TwoPartialBaseListsSource)
            .GetRunResult();

        await Assert.That(result.Diagnostics).IsEmpty();
    }
}
