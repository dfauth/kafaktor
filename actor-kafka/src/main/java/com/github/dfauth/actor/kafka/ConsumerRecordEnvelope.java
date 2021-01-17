package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class ConsumerRecordEnvelope<T> implements Envelope<T> {

    private final T payload;
    private Map<String, String> metadata;
    private final Optional<String> optSender;

    public ConsumerRecordEnvelope(T payload) {
        this(payload, Collections.emptyMap(), Optional.empty());
    }

    public ConsumerRecordEnvelope(T payload, Map<String, String> metadata) {
        this(payload, metadata, Optional.empty());
    }

    public ConsumerRecordEnvelope(T payload, Map<String, String> metadata, Optional<String> optSender) {
        this.payload = payload;
        this.metadata = metadata;
        this.optSender = optSender;
    }

    @Override
    public T payload() {
        return payload;
    }

    @Override
    public <R> Envelope<R> mapPayload(Function<T, R> f) {
        return copyOf(f.apply(payload()));
    }

    public <R> Optional<Addressable<R>> sender() {
        return optSender.map(a -> new AvroAddressable(a));
    }

    public <R> CompletableFuture<R> replyWith(Function<T, R> f) {
        R r = f.apply(payload());
        sender().ifPresent(s -> s.tell(r));
        return CompletableFuture.completedFuture(r);
    }

    private <R> Envelope<R> copyOf(R payload) {
        return new ConsumerRecordEnvelope<>(payload, metadata);
    }

}
