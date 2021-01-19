package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

public interface TopicPartitionAware<T> {
    T withTopicPartition(TopicPartition topicPartition);
}
