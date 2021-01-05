package com.github.dfauth.actor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface Addressable<T> extends Function<T, CompletableFuture<?>> {

    CompletableFuture<T> tell(Envelope<T> e);

    default CompletableFuture<T> tell(T msg) {
        return tell(Envelope.of(msg));
    }

    @Override
    default CompletableFuture<T> apply(T t) {
        return tell(t);
    }
}
