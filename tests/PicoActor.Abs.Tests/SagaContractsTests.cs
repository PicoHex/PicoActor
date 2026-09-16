namespace PicoActor.Abs.Tests;

public sealed class SagaContractsTests
{
    [Test]
    public async Task SagaCompleted_IsDomainEvent_WithResult()
    {
        var e = new SagaCompleted("done");
        await Assert.That(e).IsAssignableTo<IDomainEvent>();
        await Assert.That(e.Result).IsEqualTo("done");
    }

    [Test]
    public async Task SagaFailed_IsDomainEvent_WithReason()
    {
        var e = new SagaFailed("InvalidOperationException: boom");
        await Assert.That(e).IsAssignableTo<IDomainEvent>();
        await Assert.That(e.Reason).IsEqualTo("InvalidOperationException: boom");
    }

    [Test]
    public async Task SagaExecution_CarriesIdAndResult()
    {
        var id = Guid.CreateVersion7();
        var exec = new SagaExecution<int>(id, 42);
        await Assert.That(exec.Id).IsEqualTo(id);
        await Assert.That(exec.Result).IsEqualTo(42);
    }

    [Test]
    public async Task SagaExecutionException_CarriesSagaIdAndReason()
    {
        var id = Guid.CreateVersion7();
        var ex = new SagaExecutionException(id, "boom");
        await Assert.That(ex.SagaId).IsEqualTo(id);
        await Assert.That(ex.Reason).IsEqualTo("boom");
    }

    [Test]
    public async Task SagaResumeResult_CarriesStatusAndOptionalReason()
    {
        var id = Guid.CreateVersion7();
        var running = new SagaResumeResult(id, SagaResumeStatus.Running);
        await Assert.That(running.Reason).IsNull();
        var failed = new SagaResumeResult(id, SagaResumeStatus.Failed, "boom");
        await Assert.That(failed.Reason).IsEqualTo("boom");
    }
}
