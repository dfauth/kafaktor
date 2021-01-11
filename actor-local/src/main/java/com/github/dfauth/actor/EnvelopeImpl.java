package com.github.dfauth.actor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.nonNull;

public class EnvelopeImpl<T> implements Envelope<T> {

    private static final String ADDRESSABLE = "addressable";
    private static final String CORRELATION_ID = "correlationId";
    private final T payload;
    private final Map<String, ?> metadata;

    public static <T> Builder<T> builder(T payload) {
        return new Builder<>(payload);
    }

    public EnvelopeImpl(T t, Map<String, ?> metadata) {
        this.payload = t;
        this.metadata = metadata;
    }

    public static <T> EnvelopeImpl<T> of(T t) {
        return builder(t).build();
    }

    public static <T> EnvelopeImpl<T> of(T t, Addressable<T> addressable) {
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

        EnvelopeImpl<T> build() {
            return new EnvelopeImpl<>(payload, metadata);
        }
    }
}
