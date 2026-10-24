using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace PicoActor.Abs.Tests;

// The generated file is compiled in the CONSUMER's project: whatever language version
// that project pins applies to it. `exceptions ??= []` is a C# 12 collection expression,
// so a consumer on LangVersion=11 failed with CS9058 inside generated code it never wrote.
public sealed class FixCLanguageVersionTests
{
    private const string SubscriberSource = """
        using PicoActor.Abs;
        using PicoMediator.Abs;
        using System.Threading;
        using System.Threading.Tasks;

        public record Paid(int Id) : IDomainEvent;

        public sealed class PaidHandler : IDomainEventSubscriber<Paid>
        {
            public ValueTask Handle(DomainEventEnvelope<Paid> envelope, ICommandSender sender, CancellationToken ct)
                => ValueTask.CompletedTask;
        }
        """;

    [Test]
    public async Task GeneratedCode_CompilesAtCSharp11()
    {
        var errors = FixCGeneratorHarness.CompileWithGenerator(
            "App",
            SubscriberSource,
            LanguageVersion.CSharp11
        );

        await Assert.That(string.Join(Environment.NewLine, errors.Select(static e => e.ToString()))).IsEmpty();
    }

    [Test]
    public async Task GeneratedConfigurator_IsInternalStaticClass()
    {
        var driver = FixCGeneratorHarness.RunGenerator("App", SubscriberSource);
        var source = FixCGeneratorHarness.FindGeneratedSource(
            driver,
            FixCGeneratorHarness.HintNamePart
        );

        await Assert.That(source).IsNotNull();
        await Assert
            .That(source!)
            .Contains("internal static class PicoActorSubscriberRegistrations_");
        await Assert
            .That(source!)
            .DoesNotContain("public static class PicoActorSubscriberRegistrations_");
    }
}
