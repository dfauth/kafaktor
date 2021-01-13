package com.github.dfauth.actor.kafka;

import org.apache.avro.specific.SpecificRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;

public interface ActorMessageDespatchable {

    String ADDRESSABLE = "addressable";

    default <T extends SpecificRecord> T mapPayload(BiFunction<String, byte[], T> f) {
        return f.apply(getPayloadSchema(), getPayload().array());
    }

    ByteBuffer getPayload();

    String getPayloadSchema();

    Map<String, String> getMetadata();

}
