package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.github.dfauth.kafaktor.bootstrap.ActorKey.headIs;
import static com.github.dfauth.kafaktor.bootstrap.ActorKey.noTail;
import static com.github.dfauth.partial.Matcher.matcher;
import static com.github.dfauth.partial.PartialFunctions._case;
import static java.util.Objects.requireNonNull;

public class DelegatingActorContext<T,R> implements ParentContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(DelegatingActorContext.class);

    private final ParentContext<R> parent;
    private final String name;
    private Behavior<T> behavior;
    private Map<String, DelegatingActorContext<?,T>> children = new HashMap<>();

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
    public void processMessage(ActorKey actorKey, Envelope<T> e) {
        matcher(actorKey).matchDefault(
                _case(headIs(name).and(noTail),
                        key -> {
                            behavior = behavior.onMessage(e);
                        })
                ._case(headIs(name),
                        key -> {
                            descend(key);
                        })
                ._otherwise(() -> logger.error("unable to match actor key {}",actorKey))
        );
    }

    private Optional<ActorRef<T>> descend(ActorKey actorKey) {
        return matcher(actorKey).matchFirstOf(
                _case(headIs(name).and(noTail),
                        n -> (ActorRef<T>) children.get(n.head()).getActorRef()
                )
                ._case(headIs(name),
                        n -> {
                            return n.tail().flatMap(ak -> descend(ak)).orElseThrow();
                        }
                ));
    }

    public ActorRef<T> getActorRef() {
        return new KafkaActorRef<>(this, name);
    }
}
