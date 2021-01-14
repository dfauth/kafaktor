package com.github.dfauth.actor.kafka;

import com.github.dfauth.trycatch.Try;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.util.function.BiFunction;
import java.util.function.Function;

import static com.github.dfauth.trycatch.Try.tryWithCallable;

public interface SerializingFunction<T extends SpecificRecord> extends Serializer<T>, BiFunction<String, T,byte[]> {

    @Override
    default byte[] apply(String topic, T payload) {
        return serialize(topic, payload);
    }

    default Function<T, byte[]> withTopic(String topic) {
        return t -> apply(topic, t);
    }

    default <R extends T> Function<R, byte[]> withTopic(Class<R> topic) {
        return r -> apply(topic.getCanonicalName(), r);
    }

    default Function<T, Try<byte[]>> tryWithTopic(String topic) {
        Function<T, byte[]> _f = withTopic(topic);
        return t -> tryWithCallable(() -> _f.apply(t));
    }

    static <T extends SpecificRecordBase> SerializingFunction<T> fromSerializer(Serializer<T> serializer) {
        return (topic, data) -> serializer.serialize(topic, data);
    }
}
