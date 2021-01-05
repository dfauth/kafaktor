package com.github.dfauth.actor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Objects.nonNull;

public class Envelope<T> {

    private static final String ADDRESSABLE = "addressable";
    private static final String CORRELATION_ID = "correlationId";
    private final T payload;
    private final Map<String, ?> metadata;

    public static <T> Builder<T> builder(T payload) {
        return new Builder<>(payload);
    }

    public Envelope(T t, Map<String, ?> metadata) {
        this.payload = t;
        this.metadata = metadata;
    }

    public static <T> Envelope<T> of(T t) {
        return builder(t).build();
    }

    public static <T> Envelope<T> of(T t, Addressable<T> addressable) {
        return builder(t).withAddressable(addressable).build();
    }

    public T payload() {
        return payload;
    }

    public <R> Optional<Addressable<R>> sender() {
        return Optional.ofNullable((Addressable<R>) metadata.get(ADDRESSABLE));
    }

    public <R> CompletableFuture<R> replyWith(Function<T,R> f) {
        Optional<Addressable<R>> sender = sender();
        return sender.map(a -> a.tell(f.apply(payload))).orElse(CompletableFuture.failedFuture(new IllegalStateException("sender is not addressable")));
    }

    static class Builder<T> {
        private final T payload;
        private Map<String, Object> metadata = new HashMap<>();

        Builder(T payload) {
            nonNull(payload);
            this.payload = payload;
        }

        <R> Builder<T> withAddressable(Addressable<R> addressable) {
            nonNull(addressable);
            this.metadata.put(ADDRESSABLE, addressable);
            return this;
        }

        Builder<T> withCorrelationId() {
            return withCorrelationId(UUID.randomUUID().toString());
        }

        Builder<T> withCorrelationId(String correlationId) {
            nonNull(correlationId);
            this.metadata.put(CORRELATION_ID, correlationId);
            return this;
        }

        Envelope<T> build() {
            return new Envelope<>(payload, metadata);
        }
    }
}
