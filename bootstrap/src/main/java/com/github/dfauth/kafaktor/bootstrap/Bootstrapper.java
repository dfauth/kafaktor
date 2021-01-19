package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.BehaviorFactoryAware;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.ActorContainer;
import com.github.dfauth.kafka.RecoveryStrategy;
import com.github.dfauth.kafka.TopicPartitionAware;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.kafka.RecoveryStrategy.topicPartitionCurry;

public interface Bootstrapper<K,V,T> extends ActorContainer<K,V,T>, TopicPartitionAware<RecoveryStrategy.WithTopicPartition<K,V>> {

    static String name(TopicPartition topicPartition) {
        return String.format("%s-%d", topicPartition.topic(), topicPartition.partition());
    }

    class CachingBootstrapper<K,V,T> implements Bootstrapper<K,V,T>, ParentContext<T> {

        private static final Logger logger = LoggerFactory.getLogger(CachingBootstrapper.class);

        private final RecoveryStrategy<K, V> recoveryStrategy;
        private static final Map<String, CachingBootstrapper> instances = new HashMap<>();
        private String name = null;
        private DelegatingActorContext<T> actorContext;
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
        }

        public boolean stop() {
            return instances.remove(name, this);
        }

        @Override
        public <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name) {
            DelegatingActorContext<T> ctx = new DelegatingActorContext<>(this, name);
            DelegatingActorContext.BehaviorWithActorRef<R> b = ctx.withBehaviorFactory(behaviorFactory);
            return b.get();
        }

        @Override
        public <R> CompletableFuture<R> publish(R r) {
            return CompletableFuture.failedFuture(new IllegalStateException("Oops. not yet implemented"));
        }

        @Override
        public BehaviorFactoryAware<T, Consumer<ConsumerRecord<K, V>>> withName(String name) {
            DelegatingActorContext<T> ctx = new DelegatingActorContext<>(this, name);
            return factory -> {
                DelegatingActorContext.BehaviorWithActorRef<T> behavior = ctx.withBehaviorFactory(factory);
                return r -> {
                    logger.info("received consumer record: {}", r);
                    behavior.onMessage(recordTransformer.apply(r));
                };
            };
        }

        @Override
        public RecoveryStrategy.WithTopicPartition<K, V> withTopicPartition(TopicPartition topicPartition) {
            this.name = Bootstrapper.name(topicPartition);
            return topicPartitionCurry(recoveryStrategy).withTopicPartition(topicPartition);
        }
    }
}

