package com.github.dfauth.actor;

public interface ActorContextAware<T,R> {

    R withActorContext(ActorContext<T> ctx);
}
