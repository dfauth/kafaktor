package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.function.Function;

public interface KafkaConsumerAware<K, V, T>  extends Function<KafkaConsumer<K,V>, T> {
    @Override
    default T apply(KafkaConsumer<K, V> consumer) {
        return withKafkaConsumer(consumer);
    }

    T withKafkaConsumer(KafkaConsumer<K, V> consumer);
}
