package com.github.dfauth.actor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Addressable<T> {

    default CompletableFuture<T> tell(T t) {
        return tell(t, Optional.empty());
    }

    default CompletableFuture<T> tell(T t, Addressable<T> addressable) {
        return tell(t, Optional.ofNullable(addressable));
    }

    CompletableFuture<T> tell(T t, Optional<Addressable<T>> addressable);
}
