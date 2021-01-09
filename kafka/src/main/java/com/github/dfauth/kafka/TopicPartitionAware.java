package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.function.Function;

public interface TopicPartitionAware<T> extends Function<TopicPartition, T> {
    @Override
    default T apply(TopicPartition topicPartition) {
        return withTopicPartition(topicPartition);
    }

    T withTopicPartition(TopicPartition topicPartition);
}
