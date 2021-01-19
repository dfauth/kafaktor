package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T> implements ParentContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<T> parent;
    private final String name;

    public DelegatingActorContext(ParentContext<T> parent, String name) {
        this.parent = requireNonNull(parent);
        this.name = requireNonNull(name);
    }

    @Override
    public Optional<String> getId() {
        return Optional.of(parent.getId().map(i -> i+"/"+name).orElse("/"+name));
    }

    @Override
    public <S> ActorRef<S> spawn(Behavior.Factory<S> behaviorFactory, String name) {
        BehaviorWithActorRef<S> behavior = new DelegatingActorContext<>(this, name).withBehaviorFactory(behaviorFactory);
        return behavior.get();
    }

    @Override
    public Optional<ParentContext<T>> getParentContext() {
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

    public <R> BehaviorWithActorRef<R> withBehaviorFactory(Behavior.Factory<R> factory) {
        var ref = new Object() {
            Behavior<R> behavior = factory.withActorContext(actorContext());
        };
        return new BehaviorWithActorRef<R>() {
            @Override
            public ActorRef<R> get() {
                return new ActorRef<R>() {
                    @Override
                    public <R1> CompletableFuture<R1> ask(R r) {
                        return null;
                    }

                    @Override
                    public String id() {
                        return name;
                    }

                    @Override
                    public CompletableFuture<R> tell(R r, Optional<Addressable<R>> rAddressable) {
                        return DelegatingActorContext.this.publish(r);
                    }
                };
            }

            @Override
            public boolean isFinal() {
                return ref.behavior.isFinal();
            }

            @Override
            public Behavior<R> onMessage(Envelope<R> e) {
                ref.behavior = ref.behavior.onMessage(e);
                return this;
            }
        };
    }

    interface BehaviorWithActorRef<T> extends Behavior<T>, Supplier<ActorRef<T>> {
    }
}
