package com.github.dfauth.actor;

import java.util.function.Function;

public interface BehaviorFactory<T> extends Function<ActorContext<T>, Behavior<T>> {

    default Behavior<T> apply(ActorContext<T> ctx) {
        return create(ctx);
    }

    Behavior<T> create(ActorContext<T> ctx);
}
