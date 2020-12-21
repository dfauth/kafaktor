package com.github.dfauth.actor;

import java.util.function.Function;

public interface ActorFactory<T> extends Function<BehaviorFactory<T>, ActorImpl<T>> {

    default ActorImpl<T> apply(BehaviorFactory<T> t) {
        return create(t);
    }

    ActorImpl<T> create(BehaviorFactory<T> t);
}
