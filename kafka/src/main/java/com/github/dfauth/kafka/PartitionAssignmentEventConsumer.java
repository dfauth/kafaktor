package com.github.dfauth.kafka;

public interface PartitionAssignmentEventConsumer<K,V> extends KafkaConsumerAware<K,V,AssignmentListener> {
}
