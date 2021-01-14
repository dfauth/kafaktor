package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import com.github.dfauth.actor.kafka.ActorAddressable;
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
        private NamedBehaviorFactory<T> behaviorFactory;
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
            behavior = behaviorFactory.get();
        }

        public boolean stop() {
            return instances.remove(name, this);
        }

        @Override
        public TopicPartitionAware<RecoveryStrategy.WithTopicPartition<K,V>> apply(String name, Behavior.Factory<T> behaviorFactory) {
            this.behaviorFactory = new NamedBehaviorFactory(name, behaviorFactory);
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

    static class NamedBehaviorFactory<T> implements Supplier<Behavior<T>> {

        private final String name;
        private final Behavior.Factory<T> behaviorFactory;

        public NamedBehaviorFactory(String name, Behavior.Factory<T> behaviorFactory) {
            this.name = name;
            this.behaviorFactory = behaviorFactory;
        }

        @Override
        public Behavior<T> get() {
            return behaviorFactory.apply(new NestedActorContext(name));
        }

        private class NestedActorContext<T> implements ActorContext<T>, ActorRef<T> {

            private final String name;

            public NestedActorContext(String name) {
                this.name = name;
            }

            @Override
            public <R> CompletableFuture<R> ask(T t) {
                return null;
            }

            @Override
            public String id() {
                return name;
            }

            @Override
            public ActorRef<T> self() {
                return this;
            }

            @Override
            public <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name) {
                NestedActorContext<R> ctx = new NestedActorContext<R>(name);
                behaviorFactory.apply(ctx);
                return ctx;
            }

            @Override
            public Logger getLogger() {
                return getLogger();
            }

            @Override
            public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
                CompletableFuture<T> f = new CompletableFuture<T>();
                ConsumerRecordEnvelope<T> e = new ConsumerRecordEnvelope<T>(t);
                ConsumerRecordEnvelope<T> result = optAddressable.map(a -> e.withAddressable(a)).orElseGet(() -> e.withAddressable(new ActorAddressable<T>(name)));
                return f;
            }
        }
    }
}

