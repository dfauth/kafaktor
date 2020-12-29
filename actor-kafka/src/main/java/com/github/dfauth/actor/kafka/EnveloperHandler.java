package com.github.dfauth.actor.kafka;

import com.github.dfauth.partial.Tuple2;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
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

    static <T extends SpecificRecordBase> Tuple2<Map<String, String>, T> extract(ActorMessage actorMessage, Deserializer<T> deserializer) {
        return new Tuple2<>() {
            @Override
            public Map<String, String> _1() {
                return actorMessage.getMetadata();
            }

            @Override
            public T _2() {
                return payload(actorMessage, deserializer);
            }
        };
    }

    static <T extends SpecificRecordBase> T payload(ActorMessage actorMessage, Deserializer<T> deserializer) {
        return deserializer.deserialize(actorMessage.getPayloadSchema(), actorMessage.getPayload().array());
    }
}
