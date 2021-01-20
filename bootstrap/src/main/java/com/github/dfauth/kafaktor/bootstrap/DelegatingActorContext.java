package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T,R> implements ParentContext<R> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<R> parent;
    private final String name;
    private final Behavior<T> behavior;

    public DelegatingActorContext(ParentContext<R> parent, String name, Behavior.Factory<T> behaviorFactory) {
        this.parent = requireNonNull(parent);
        this.name = requireNonNull(name);
        this.behavior = behaviorFactory.withActorContext(actorContext());
    }

    @Override
    public Optional<String> getId() {
        return Optional.of(parent.getId().map(i -> i+"/"+name).orElse("/"+name));
    }

    @Override
    public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
        DelegatingActorContext<S,R> _behavior = new DelegatingActorContext<S,R>(this, name, behaviorFactory);
        return null;
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

    public ActorRef<T> getActorRef() {
        return new ActorRef<T>() {
            @Override
            public <R> CompletableFuture<R> ask(T t) {
                return publish(t).thenApply(_t -> (R)null);
            }

            @Override
            public String id() {
                return name;
            }

            @Override
            public CompletableFuture<T> tell(T t, Optional<Addressable<T>> tAddressable) {
                return publish(t);
            }
        };
    }

    interface BehaviorWithActorRef<T> extends Behavior<T> {
        ActorRef<T> getActorRef();
    }
}
