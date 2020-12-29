package com.github.dfauth.actor;

import java.util.function.Function;

public interface ActorFactory<T> extends Function<Behavior.Factory<T>, ActorImpl<T>> {

    default ActorImpl<T> apply(Behavior.Factory<T> t) {
        return create(t);
    }

    ActorImpl<T> create(Behavior.Factory<T> t);
}
