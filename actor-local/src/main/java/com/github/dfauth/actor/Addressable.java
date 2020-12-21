package com.github.dfauth.actor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface Addressable<T> extends Function<T, CompletableFuture<?>> {

    CompletableFuture<?> tell(Envelope<T> e);

    default CompletableFuture<?> tell(T msg) {
        return tell(Envelope.of(msg));
    }

    @Override
    default CompletableFuture<?> apply(T t) {
        return tell(t);
    }
}
