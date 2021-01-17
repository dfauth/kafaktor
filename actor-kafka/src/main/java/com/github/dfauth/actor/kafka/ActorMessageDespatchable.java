package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface ActorMessageDespatchable {

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

    default <T> Class<T> getPayloadType() {
        return tryCatch(() -> (Class<T>) Class.forName(getPayloadSchema()));
    }

    default <T extends SpecificRecordBase> Envelope<T> asEnvelope(DeserializingFunction<T> f) {
        return new ConsumerRecordEnvelope(f.apply(getPayloadSchema(), getPayload().array()), getMetadata());
    }

    interface Builder<E extends Builder<E>> {
    }

}
