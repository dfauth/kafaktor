package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface RecoveryStrategy<K,V> {

    default void invoke(KafkaConsumer<K,V> c, TopicPartition p) {
        invoke(c,p,() -> Instant.ofEpochMilli(0));
    }

    void invoke(KafkaConsumer<K,V> c, TopicPartition p, Supplier<Instant> supplier);

    static <K,V> Curried<K,V> curried(RecoveryStrategy<K,V> recoveryStrategy) {
        return c -> tp -> s -> recoveryStrategy.invoke(c, tp, s);
    }

    static <K,V> TopicPartitionAware<WithTopicPartition<K,V>> topicPartitionCurry(RecoveryStrategy<K,V> recoveryStrategy) {
        return tp -> c -> curried(recoveryStrategy).apply(c).apply(tp);
    }

    interface Curried<K,V> extends KafkaConsumerAware<K,V, TopicPartitionAware<Consumer<Supplier<Instant>>>> {
        default void invoke(KafkaConsumer<K,V> c, TopicPartition p) {
            apply(c).apply(p).accept(() -> Instant.ofEpochMilli(0));
        }
    }

    interface WithTopicPartition<K,V> extends KafkaConsumerAware<K,V,Consumer<Supplier<Instant>>> {
        default void invoke(KafkaConsumer<K,V> c, Supplier<Instant> s) {
            apply(c).accept(s);
        }
    }
}
