package com.github.dfauth.actor;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface Envelope<T> {

    T payload();

    <R> Envelope<R> mapPayload(Function<T,R> f);

    <R> Optional<Addressable<R>> sender();

    <R> CompletableFuture<R> replyWith(Function<T,R> f);
}
