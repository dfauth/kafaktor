package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public interface RecoveryStrategy<K,V> {

    void invoke(KafkaConsumer<K,V> c, TopicPartition p);
}
