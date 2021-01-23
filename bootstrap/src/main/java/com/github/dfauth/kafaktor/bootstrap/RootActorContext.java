package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class RootActorContext<T> implements ParentContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(RootActorContext.class);

    private final String name;
    private Behavior<T> guardianBehavior;
    private Map<String, DelegatingActorContext<?,T>> children = new HashMap<>();
    private Publisher publisher;
    private String topic;

    public RootActorContext(String topic, Behavior.Factory<T> guardianBehaviorFactory, Publisher publisher, String name) {
        this.topic = topic;
        this.name = requireNonNull(name);
        this.guardianBehavior = guardianBehaviorFactory.withActorContext(actorContext());
        this.publisher = publisher;
    }

    @Override
    public <R> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<T,?>> optSender) {
        return publisher.publish(recipient, msg, optSender);
    }

    @Override
    public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
        return (ActorRef<S>) children.compute(name, (k, v) -> new DelegatingActorContext<>(this, name, behaviorFactory)).getActorRef();
    }

    public <R> ActorContext<R> actorContext() {
        return new ActorContext<>() {
            @Override
            public String id() {
                return RootActorContext.this.getId().orElseThrow();
            }

            @Override
            public ActorRef<R> self() {
                return (ActorRef<R>) RootActorContext.this;
            }

            @Override
            public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
                return RootActorContext.this.spawn(behaviorFactory, name);
            }

            @Override
            public Logger getLogger() {
                return RootActorContext.this.logger;
            }
        };
    }

    public boolean stop() {
        return false;
    }

    @Override
    public Optional<ParentContext<T>> findActor(K key, Class<T> expectedType) {
        return Optional.empty();
    }

    @Override
    public void onMessage(Envelope<T> e) {
        guardianBehavior = guardianBehavior.onMessage(e);
    }

    @Override
    public String getTopic() {
        return topic;
    }
}
