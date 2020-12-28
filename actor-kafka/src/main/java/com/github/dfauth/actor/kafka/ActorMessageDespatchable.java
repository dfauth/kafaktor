package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.kafka.bootstrap.Despatchable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

public interface ActorMessageDespatchable {

    default <T extends Despatchable> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    ByteBuffer getPayload();

    String getPayloadSchema();

    static <T extends SpecificRecordBase> ActorMessage envelope(String key, T record, Serializer<T> serializer) {
        return envelope(key, Collections.emptyMap(), record, serializer);
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(String key, Map<String, String> metadata, T record, Serializer<T> serializer) {
        return ActorMessage.newBuilder().setPayload(ByteBuffer.wrap(serializer.serialize(record.getSchema().getFullName(), record)))
                .setPayloadSchema(record.getSchema().getFullName()).setTimestamp(Instant.now().toEpochMilli()).setKey(key).setMetadata(metadata).build();
    }

}
