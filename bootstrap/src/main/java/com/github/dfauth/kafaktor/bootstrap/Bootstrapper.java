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
        private String name = null;
        private Function<ConsumerRecord<K,V>, Envelope<?>> recordTransformer;

        public static final Optional<ParentContext> lookup(String name) {
            return Optional.ofNullable(instances.get(name));
        }

        public CachingBootstrapper(RecoveryStrategy<K, V> recoveryStrategy, Function<ConsumerRecord<K,V>, Envelope<?>> recordTransformer) {
            this.recoveryStrategy = recoveryStrategy;
            this.recordTransformer = recordTransformer;
        }

        public <T> void createActorSystem(String name, Behavior.Factory<T> guardianBehavior) {
            instances.put(name, new RootActorContext<>(name, guardianBehavior));
        }

        public boolean stop() {
            return Optional.ofNullable(instances.remove(name)).map(ctx -> ctx.stop()).orElse(false);
        }

//        @Override
//        public BehaviorFactoryAware.Consumer<T> withName(String name) {
//            DelegatingActorContext<T> ctx = new DelegatingActorContext<>(instances.get(name), name);
//            return factory -> {
//                DelegatingActorContext.BehaviorWithActorRef<T> behavior = ctx.withBehaviorFactory(factory);
//            };
//        }

        @Override
        public Offset process(ConsumerRecord<K, V> r) {
            logger.info("received consumer record: {}", r);
            Offset result = () -> r.offset() + 1;
            return Optional.ofNullable(instances.get(name(r)))
//                    .map(ctx -> ctx.findActor(r.key()))
                    .map(ctx -> tryCatch(() -> {
                            ctx.onMessage(recordTransformer.apply(r));
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

