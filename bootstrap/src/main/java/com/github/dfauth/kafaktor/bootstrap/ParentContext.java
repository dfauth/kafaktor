package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ParentContext<T> {

    default Optional<String> getId() {
        return Optional.empty();
    }

    <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name);

    default <R> CompletableFuture<R> publish(R msg) {
        // either provide a parental context to delagate to
        // or implement publishing
        return getParentContext()
                .map(c -> c.publish(msg))
                .orElse(CompletableFuture.failedFuture(new IllegalStateException("No implementation of publish provided")));
    }

    default Optional<ParentContext<T>> getParentContext() {
        return Optional.empty();
    }
}
