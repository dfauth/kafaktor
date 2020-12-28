package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.kafka.bootstrap.Despatchable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

public interface ActorMessageDespatchable extends EnveloperHandler {

    default <T extends Despatchable> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    ByteBuffer getPayload();

    String getPayloadSchema();
}
