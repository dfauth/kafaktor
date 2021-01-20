package com.github.dfauth.actor.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Function;

public interface ConsumerRecordProcessor<K, V> extends Function<ConsumerRecord<K, V>, Long> {
    @Override
    default Long apply(ConsumerRecord<K, V> r) {
        return process(r).next();
    }

    Offset process(ConsumerRecord<K, V> r);
}
