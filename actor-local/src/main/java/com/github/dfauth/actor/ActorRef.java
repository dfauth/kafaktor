package com.github.dfauth.actor;

import java.util.concurrent.CompletableFuture;

public interface ActorRef<T> extends Addressable<T> {

    CompletableFuture<T> ask(T t);

    String id();
}
