package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;
import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface ActorMessageDespatchable extends Envelope<ByteBuffer> {

    String ADDRESSABLE = "addressable";

    default <T extends SpecificRecord> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    ByteBuffer getPayload();

    String getPayloadSchema();

    Map<String, String> getMetadata();

    default ByteBuffer payload() {
        return getPayload();
    }

    default <R> Optional<Addressable<R>> sender() {
        return Optional.ofNullable(getMetadata().get(ADDRESSABLE)).map(a -> new AvroAddressable(a));
    }

    default <R> CompletableFuture<R> replyWith(Function<ByteBuffer,R> f) {
        R r = f.apply(getPayload());
        sender().ifPresent(s -> s.tell(r));
        return CompletableFuture.completedFuture(r);
    }

}
