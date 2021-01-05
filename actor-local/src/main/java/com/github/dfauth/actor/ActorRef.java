package com.github.dfauth.actor;

import java.util.concurrent.CompletableFuture;

public interface ActorRef<T> extends Addressable<T> {

    <R> CompletableFuture<R> ask(T t);

    String id();
}
