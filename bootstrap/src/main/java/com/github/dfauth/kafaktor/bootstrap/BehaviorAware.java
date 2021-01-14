package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.Behavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

interface BehaviorAware<T,R> extends Function<Behavior<T>, R> {
    @Override
    default R apply(Behavior<T> behavior) {
        return withBehavior(behavior);
    }

    R withBehavior(Behavior<T> behavior);
}
