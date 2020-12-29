package com.github.dfauth.actor;

import java.util.function.Function;

public interface ActorContextAware<T,R> extends Function<ActorContext<T>,R> {

    @Override
    default R apply(ActorContext<T> ctx) {
        return withActorContext(ctx);
    }

    R withActorContext(ActorContext<T> ctx);
}
