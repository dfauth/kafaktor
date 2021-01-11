package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.function.Supplier;

public interface RecoveryStrategy<K,V> {

    default void invoke(KafkaConsumer<K,V> c, TopicPartition p) {
        invoke(c,p,() -> Instant.ofEpochMilli(0));
    }

    void invoke(KafkaConsumer<K,V> c, TopicPartition p, Supplier<Instant> supplier);
}
