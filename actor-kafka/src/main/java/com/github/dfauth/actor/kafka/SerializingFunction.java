package com.github.dfauth.actor.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.function.BiFunction;

public interface SerializingFunction<T extends SpecificRecord> extends Serializer<T>, BiFunction<String, T,byte[]> {

    @Override
    default byte[] apply(String topic, T payload) {
        return serialize(topic, payload);
    }
}
