package dfauth.kafaktor.bootstrap;

import com.github.dfauth.kafka.OffsetManager;

public class RecoveryStrategies<K,V> {

    public static final <K,V> RecoveryStrategy<K,V> timeBased() {
        return (c, p, supplier) -> {
            OffsetManager.Utils.<K,V>timeBased().withKafkaConsumer(c)
                    .withTopicPartition(p)
                    .accept(supplier);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> seekToStart() {
        return (c, p, supplier) -> {
            OffsetManager.Utils.<K,V>seekToStart().withKafkaConsumer(c)
                    .withTopicPartition(p)
                    .accept(supplier);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> current() {
        return (c, p, supplier) -> {
            OffsetManager.Utils.<K,V>current().withKafkaConsumer(c)
                    .withTopicPartition(p)
                    .accept(supplier);
        };
    }

    public static final <K,V> RecoveryStrategy<K,V> seekToEnd() {
        return (c, p, supplier) -> {
            OffsetManager.Utils.<K,V>seekToEnd().withKafkaConsumer(c)
                    .withTopicPartition(p)
                    .accept(supplier);
        };
    }
}
