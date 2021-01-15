package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface ActorMessageDespatchable<E extends ActorMessageDespatchable<E>> {

    String ADDRESSABLE = "addressable";

    default <T extends SpecificRecord> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    String getRecipient();

    String getSender();

    default <T extends SpecificRecordBase> Optional<Addressable<T>> getOptSender() {
        return Optional.ofNullable(getSender()).map(s -> new AvroAddressable<T>(s));
    }

    ByteBuffer getPayload();

    String getPayloadSchema();

    Map<String, String> getMetadata();

    default <T> ActorMessageDespatchable<E> inspectPayload(BiFunction<String, byte[], T> f, Consumer<Envelope<T>> c) {
        c.accept(new ConsumerRecordEnvelope(f.apply(getPayloadSchema(), getPayload().array()), getMetadata()));
        return this;
    }
}
