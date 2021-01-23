package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T,R> implements ParentContext<R> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<R> parent;
    private final String name;
    private final Behavior<T> behavior;
    private Optional<String> optTopic;

    public DelegatingActorContext(ParentContext<R> parent, String name, Behavior.Factory<T> behaviorFactory) {
        this(parent, name, behaviorFactory, Optional.empty());
    }

    public DelegatingActorContext(ParentContext<R> parent, String name, Behavior.Factory<T> behaviorFactory, String topic) {
        this(parent, name, behaviorFactory, Optional.ofNullable(topic));
    }

    public DelegatingActorContext(ParentContext<R> parent, String name, Behavior.Factory<T> behaviorFactory, Optional<String> optTopic) {
        this.parent = requireNonNull(parent);
        this.name = requireNonNull(name);
        this.behavior = behaviorFactory.withActorContext(actorContext());
        this.optTopic = optTopic;
    }

    @Override
    public Optional<String> getId() {
        return Optional.of(parent.getId().map(i -> i+"/"+name).orElse("/"+name));
    }

    @Override
    public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
        DelegatingActorContext<S,R> ctx = new DelegatingActorContext<S,R>(this, name, behaviorFactory);
        return ctx.getActorRef();
    }

    @Override
    public Optional<ParentContext<R>> getParentContext() {
        return Optional.of(parent);
    }

    public <R> ActorContext<R> actorContext() {
        return new ActorContext<>() {
            @Override
            public String id() {
                return DelegatingActorContext.this.getId().orElseThrow();
            }

            @Override
            public ActorRef<R> self() {
                return (ActorRef<R>) DelegatingActorContext.this;
            }

            @Override
            public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
                return DelegatingActorContext.this.spawn(behaviorFactory, name);
            }

            @Override
            public Logger getLogger() {
                return DelegatingActorContext.this.logger;
            }
        };
    }

    public boolean stop() {
        return false;
    }

    @Override
    public Optional<ParentContext<R>> findActor(K key, Class<R> expectedType) {
        return Optional.empty();
    }

    @Override
    public void onMessage(Envelope<R> apply) {

    }

    @Override
    public String getTopic() {
        return optTopic.orElse(parent.getTopic());
    }

    public ActorRef<T> getActorRef() {
        return new KafkaActorRef<>(this, name, optTopic);
    }
}
