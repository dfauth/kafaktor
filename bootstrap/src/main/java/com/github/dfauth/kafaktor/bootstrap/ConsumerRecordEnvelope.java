package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.AvroAddressable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.github.dfauth.actor.kafka.ActorMessageDespatchable.ADDRESSABLE;

public class ConsumerRecordEnvelope<T> implements Envelope<T> {

    private final T payload;
    private Map<String, String> metadata;

    public static <T extends SpecificRecordBase> ConsumerRecordEnvelope<T> create(Function<byte[], T> f, ConsumerRecord<String, ActorMessage> r) {
        return new ConsumerRecordEnvelope<T>(f.apply(r.value().getPayload().array()), r.value().getMetadata());
    }

    public ConsumerRecordEnvelope(T payload, Map<String, String> metadata) {
        this.payload = payload;
        this.metadata = metadata;
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
        return Optional.ofNullable(metadata.get(ADDRESSABLE)).map(a -> new AvroAddressable(a));
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
