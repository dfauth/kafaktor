package com.github.dfauth.actor.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.function.BiFunction;

public interface DeserializingFunction<T extends SpecificRecord> extends Deserializer<T>, BiFunction<String, byte[], T> {

    @Override
    default T apply(String topic, byte[] payload) {
        return deserialize(topic, payload);
    }
}
