package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

import static com.github.dfauth.Lists.segment;
import static com.github.dfauth.partial.Extractable._case;
import static com.github.dfauth.partial.Matcher.match;
import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T,R> implements ParentContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<R> parent;
    private final String name;
    private Behavior<T> behavior;

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
        DelegatingActorContext<S, T> ctx = new DelegatingActorContext<>(this, name, behaviorFactory);
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
    public void processMessage(String address, Envelope<T> e) {
        match(segment(Arrays.asList(address.split("/")))).using(
                _case((h, t) -> h.equals(name) && t.isEmpty(),
                        (h, t) -> {
                            behavior = behavior.onMessage(e);
                        })
//                _case(t -> t._1().equals(name) && t._2().isEmpty(),
//                        ignored -> {
//                            behavior = behavior.onMessage(e);
//                        }),
//                _case(t -> t._1().equals(name),
//                        ignored -> {
//                            // find actor
//                        }
        );
    }

    public ActorRef<T> getActorRef() {
        return new KafkaActorRef<>(this, name);
    }
}
