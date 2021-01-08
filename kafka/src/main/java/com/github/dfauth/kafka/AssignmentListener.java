package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.function.Consumer;

public interface AssignmentListener extends Consumer<Collection<TopicPartition>> {

    @Override
    default void accept(Collection<TopicPartition> partitions) {
        onAssignment(partitions);
    }

    void onAssignment(Collection<TopicPartition> partitions);
}

interface ConsumerAssignmentListener<K,V> extends KafkaConsumerAware<K,V,AssignmentListener> {
}
