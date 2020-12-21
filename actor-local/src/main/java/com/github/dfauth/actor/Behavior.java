package com.github.dfauth.actor;

import java.util.function.Function;

public interface Behavior<T> extends Function<Envelope<T>, Behavior<T>> {

    default Behavior<T> apply(Envelope<T> e) {
        return onMessage(e);
    }

    default boolean isFinal() {
        return false;
    }

    Behavior<T> onMessage(Envelope<T> e);
}
