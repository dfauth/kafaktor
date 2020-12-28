package com.github.dfauth.actor.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

public interface EnveloperHandler {

    static <T extends SpecificRecordBase> ActorMessage envelope(String key, T record, Serializer<T> serializer) {
        return envelope(key, Collections.emptyMap(), record, serializer);
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(String key, Map<String, String> metadata, T record, Serializer<T> serializer) {
        return ActorMessage.newBuilder()
                .setTimestamp(Instant.now().toEpochMilli())
                .setKey(key)
                .setMetadata(metadata)
                .setPayloadSchema(record.getSchema().getFullName())
                .setPayload(ByteBuffer.wrap(serializer.serialize(record.getSchema().getFullName(), record)))
                .build();
    }
}
