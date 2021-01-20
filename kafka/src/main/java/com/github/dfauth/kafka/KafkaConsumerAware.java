package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaConsumerAware<K, V, T> {
    T withKafkaConsumer(KafkaConsumer<K, V> consumer);

    interface Consumer<K,V> extends KafkaConsumerAware<K,V,Void> {
        @Override
        default Void withKafkaConsumer(KafkaConsumer<K, V> c) {
            acceptKafkaConsumer(c);
            return null;
        }

        void acceptKafkaConsumer(KafkaConsumer<K, V> c);
    }
}
