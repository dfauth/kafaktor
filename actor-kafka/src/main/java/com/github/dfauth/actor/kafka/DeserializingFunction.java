package com.github.dfauth.actor.kafka;

import com.github.dfauth.trycatch.Try;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.github.dfauth.trycatch.Try.tryWithCallable;

public interface DeserializingFunction<T extends SpecificRecord> extends Deserializer<T>, BiFunction<String, byte[], T> {

    @Override
    default T apply(String topic, byte[] payload) {
        return deserialize(topic, payload);
    }

    default Function<byte[], T> withTopic(String topic) {
        return bytes -> apply(topic, bytes);
    }

    default <R extends SpecificRecordBase> Function<byte[], R> withTopic(Class<R> topic) {
        return bytes -> (R) apply(topic.getCanonicalName(), bytes);
    }

    default Function<byte[], Try<T>> tryWithTopic(String topic) {
        Function<byte[], T> _f = withTopic(topic);
        return bytes -> tryWithCallable(() -> _f.apply(bytes));
    }

    static <T extends SpecificRecordBase> DeserializingFunction<T> fromDeserializer(Deserializer<T> deserializer) {
        return (topic, data) -> deserializer.deserialize(topic, data);
    }
}
