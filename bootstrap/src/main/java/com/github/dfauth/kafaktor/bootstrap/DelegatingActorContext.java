package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T, R> implements ParentContext<R>, ActorRef<T>, Behavior<T> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<R> parent;
    private final String name;
    private final Behavior.Factory<T> behaviorFactory;
    private Behavior<T> behavior;

    public DelegatingActorContext(ParentContext<R> parent, String name, Behavior.Factory<T> behaviorFactory) {
        this.parent = requireNonNull(parent);
        this.name = requireNonNull(name);
        this.behaviorFactory = requireNonNull(behaviorFactory);
    }

    public DelegatingActorContext<T, R> start() {
        behavior = this.behaviorFactory.withActorContext(actorContext());
        return this;
    }

    @Override
    public <R> CompletableFuture<R> ask(T t) {
        return (CompletableFuture<R>) parent.publish(t);
    }

    @Override
    public String id() {
        return getId().orElseThrow(() -> new IllegalStateException("Will never happen"));
    }

    @Override
    public Optional<String> getId() {
        return Optional.of(parent.getId().map(i -> i+"/"+name).orElse("/"+name));
    }

    @Override
    public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
        return new DelegatingActorContext<>(this, name, behaviorFactory).start();
    }

    @Override
    public Optional<ParentContext<R>> getParentContext() {
        return Optional.of(parent);
    }

    @Override
    public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
        return publish(t);
    }

    public <R> ActorContext<R> actorContext() {
        return new ActorContext<>() {
            @Override
            public String id() {
                return DelegatingActorContext.this.id();
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

    @Override
    public boolean isFinal() {
        return behavior.isFinal();
    }

    @Override
    public Behavior<T> onMessage(Envelope<T> e) {
        return behavior.onMessage(e);
    }
}
