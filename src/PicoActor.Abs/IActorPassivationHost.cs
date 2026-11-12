namespace PicoActor.Abs;

/// <summary>
/// Optional companion of <see cref="IActorSystem"/>: the stop request a PASSIVATION makes on
/// behalf of one specific actor instance.
/// <para>
/// <see cref="IActorSystem.RequestStop"/> removes by id, which is correct for a caller that has
/// no instance at all (an operator, a saga terminating itself). It is wrong for a queued
/// passivation sentinel: the sentinel is consumed by a loop, while the id it names may already
/// belong to a rebuilt replacement. An id-only removal would then unregister that LIVE actor (its
/// loop keeps running, and nothing can reach it again) while the actor that actually wanted to go
/// away stays registered.
/// </para>
/// <para>
/// Deliberately NOT a member of <see cref="IActorSystem"/>: adding it there would break every
/// existing implementation, and only a host that keeps the registry can answer it. An actor asks
/// through this interface when its system provides it and falls back to
/// <see cref="IActorSystem.RequestStop"/> when it does not.
/// </para>
/// </summary>
public interface IActorPassivationHost
{
    /// <summary>
    /// Stop <paramref name="expected"/> and remove it from the registry — but only if it is STILL
    /// the instance registered for <paramref name="id"/>. Returns whether it was (false: the id
    /// is gone or belongs to another instance, so nothing was stopped or removed).
    /// </summary>
    bool TryRequestStop(Guid id, IActor expected);
}
