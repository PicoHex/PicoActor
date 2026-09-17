# samples

## PicoActor.Sample

Event-Sourced Counter demonstration.

Covers: `ActorSystem`, `InMemoryEventStore`, `CreateAsync`, `Send`,
`AskAsync`, `OutputChannel`, `StopAsync`, `GetAsync` (event-sourced
rebuild), idempotent StopAsync — plus the saga + event-outflow part
(`ExecuteSaga`, `IDomainEventSubscriber<TEvent>`, `ResumeInterruptedSagasAsync`,
PicoDI/PicoMediator wiring).

```bash
dotnet run --project samples/PicoActor.Sample
```
