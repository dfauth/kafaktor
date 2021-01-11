package com.github.dfauth.actor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface Addressable<T> extends Function<T, CompletableFuture<?>> {

    default CompletableFuture<T> tell(T t) {
        return tell(t, Optional.empty());
    }

    default CompletableFuture<T> tell(T t, Addressable<T> addressable) {
        return tell(t, Optional.ofNullable(addressable));
    }

    CompletableFuture<T> tell(T t, Optional<Addressable<T>> addressable);

    @Override
    default CompletableFuture<T> apply(T t) {
        return tell(t);
    }
}
