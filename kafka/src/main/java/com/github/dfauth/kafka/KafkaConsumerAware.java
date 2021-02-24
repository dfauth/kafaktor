package com.github.dfauth.kafka;

import com.github.dfauth.partial.Unit;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import static com.github.dfauth.partial.Unit.UNIT;

public interface KafkaConsumerAware<K, V, T> {
    T withKafkaConsumer(KafkaConsumer<K, V> consumer);

    interface Consumer<K,V> extends KafkaConsumerAware<K,V, Unit> {
        @Override
        default Unit withKafkaConsumer(KafkaConsumer<K, V> c) {
            acceptKafkaConsumer(c);
            return UNIT;
        }

        void acceptKafkaConsumer(KafkaConsumer<K, V> c);
    }
}
