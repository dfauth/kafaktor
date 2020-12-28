package com.github.dfauth.actor.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.function.BiFunction;

public interface ActorMessageDespatchable {

    default <T> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    ByteBuffer getPayload();

    String getPayloadSchema();

    static ActorMessage envelope(String key, SpecificRecordBase record, Serializer<Object> serializer) {
        return ActorMessage.newBuilder().setPayload(ByteBuffer.wrap(serializer.serialize(record.getSchema().getFullName(), record)))
                .setPayloadSchema(record.getSchema().getFullName()).setTimestamp(Instant.now().toEpochMilli()).setKey(key).setMetadata(Collections.emptyMap()).build();
    }

}
