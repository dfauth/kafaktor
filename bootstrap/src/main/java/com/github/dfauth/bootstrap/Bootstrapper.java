package com.github.dfauth.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.kafka.RecoveryStrategy;
import com.github.dfauth.kafka.TopicPartitionAware;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.kafka.RecoveryStrategy.topicPartitionCurry;

public interface Bootstrapper<K,V,T> extends BiFunction<String, Behavior.Factory<T>, TopicPartitionAware<RecoveryStrategy.WithTopicPartition<K,V>>>, Consumer<ConsumerRecord<K,V>> {

    static String name(TopicPartition topicPartition) {
        return String.format("%s-%d", topicPartition.topic(), topicPartition.partition());
    }

    class CachingBootstrapper<K,V,T> implements Bootstrapper<K,V,T> {

        private static final Logger logger = LoggerFactory.getLogger(CachingBootstrapper.class);

        private final RecoveryStrategy<K, V> recoveryStrategy;
        private static final Map<String, CachingBootstrapper> instances = new HashMap<>();
        private String name = null;
        private Behavior.Factory<T> behaviorFactory;
        private Behavior<T> behavior;
        private Function<ConsumerRecord<K,V>, Envelope<T>> recordTransformer;

        public static final Optional<CachingBootstrapper> lookup(TopicPartition topicPartition) {
            return Optional.ofNullable(instances.get(Bootstrapper.name(topicPartition)));
        }

        public CachingBootstrapper(RecoveryStrategy<K, V> recoveryStrategy, Function<ConsumerRecord<K,V>, Envelope<T>> recordTransformer) {
            this.recoveryStrategy = recoveryStrategy;
            this.recordTransformer = recordTransformer;
        }

        public void start() {
            instances.put(name, this);
            behavior = behaviorFactory.apply(new ActorContext<T>() {
                @Override
                public String id() {
                    return null;
                }

                @Override
                public ActorRef<T> self() {
                    return null;
                }

                @Override
                public <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name) {
                    return null;
                }

                @Override
                public Logger getLogger() {
                    return logger;
                }
            });
        }

        public boolean stop() {
            return instances.remove(name, this);
        }

        @Override
        public TopicPartitionAware<RecoveryStrategy.WithTopicPartition<K,V>> apply(String name, Behavior.Factory<T> behaviorFactory) {
            this.behaviorFactory = behaviorFactory;
            return tp -> {
                this.name = Bootstrapper.name(tp);
                return topicPartitionCurry(recoveryStrategy).apply(tp);
            };
        }

        @Override
        public void accept(ConsumerRecord<K, V> r) {
            logger.info("received consumer record: {}",r);
            behavior.onMessage(recordTransformer.apply(r));
        }
    }
}

