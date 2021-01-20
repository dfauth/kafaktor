package com.github.dfauth.kafka;

public class RecoveryStrategies {

    public static final <K,V> TimestampAware.RecoveryStrategy<K,V> timeBased() {
        return i -> (c, p) -> {
            OffsetManager.Utils.<K,V>timeBased(i).withKafkaConsumer(c)
                    .withTopicPartition(p);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> seekToStart() {
        return (c, p) -> {
            OffsetManager.Utils.<K,V>seekToStart().withKafkaConsumer(c)
                    .withTopicPartition(p);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> current() {
        return (c, p) -> {
            OffsetManager.Utils.<K,V>current().withKafkaConsumer(c)
                    .withTopicPartition(p);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> seekToEnd() {
        return (c, p) -> {
            OffsetManager.Utils.<K,V>seekToEnd().withKafkaConsumer(c)
                    .withTopicPartition(p);
        };
    }
}
