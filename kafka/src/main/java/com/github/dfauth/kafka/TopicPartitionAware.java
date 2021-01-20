package com.github.dfauth.kafka;

import org.apache.kafka.common.TopicPartition;

public interface TopicPartitionAware<T> {
    T withTopicPartition(TopicPartition topicPartition);

    interface Consumer extends TopicPartitionAware<Void> {
        @Override
        default Void withTopicPartition(TopicPartition topicPartition) {
            acceptTopicPartition(topicPartition);
            return null;
        }

        void acceptTopicPartition(TopicPartition topicPartition);
    }
}
