package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.ConsumerRecordProcessor;
import com.github.dfauth.actor.kafka.Offset;
import com.github.dfauth.kafka.RecoveryStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.dfauth.kafaktor.bootstrap.ActorKey.toActorKey;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface Bootstrapper<K,V> extends ConsumerRecordProcessor<K,V> {

    static String name(TopicPartition topicPartition) {
        return name(topicPartition.topic(), topicPartition.partition());
    }

    static <K,V> String name(ConsumerRecord<K,V> r) {
        return name(r.topic(), r.partition());
    }

    static String name(String topic, int partition) {
        return String.format("%s-%d", topic, partition);
    }

    class CachingBootstrapper<K,V> implements Bootstrapper<K,V> {

        private static final Logger logger = LoggerFactory.getLogger(CachingBootstrapper.class);

        private final RecoveryStrategy<K, V> recoveryStrategy;
        private static final Map<String, ParentContext> instances = new HashMap<>();
        private Function<ConsumerRecord<K,V>, Envelope<?>> recordTransformer;
        private String name;

        public static final Optional<ParentContext> lookup(String name) {
            return Optional.ofNullable(instances.get(name));
        }

        public CachingBootstrapper(RecoveryStrategy<K, V> recoveryStrategy, Function<ConsumerRecord<K,V>, Envelope<?>> recordTransformer) {
            this.recoveryStrategy = recoveryStrategy;
            this.recordTransformer = recordTransformer;
        }

        public <T> void createActorSystem(String topic, int partition, String name, Behavior.Factory<T> guardianBehavior, Publisher publisher) {
            instances.put(name(topic, partition), new RootActorContext<>(topic, name, guardianBehavior, publisher));
        }

        public boolean stop() {
            return Optional.ofNullable(instances.remove(name)).map(ctx -> ctx.stop()).orElse(false);
        }

        @Override
        public Offset process(ConsumerRecord<K, V> r) {
            logger.info("received consumer record: {}", r);
            Offset result = () -> r.offset() + 1;
            return Optional.ofNullable(instances.get(name(r)))
                    .map(ctx -> tryCatch(() -> {
                            ctx.processMessage(toActorKey((String) r.key()), recordTransformer.apply(r));
                            return result;
                        }, ignored -> result)
                    ).orElseGet(() -> {
                        logger.error("No actor found for key {}",r.key());
                        return result;
            });
        }

        public RecoveryStrategy<K, V> getRecoveryStrategy() {
            return recoveryStrategy;
        }

    }
}

