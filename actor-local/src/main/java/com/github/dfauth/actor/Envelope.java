package com.github.dfauth.actor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.nonNull;

public interface Envelope<T> {

    T payload();

    <R> Optional<Addressable<R>> sender();

    <R> CompletableFuture<R> replyWith(Function<T,R> f);
}
