package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.dfauth.Lists.segment;
import static com.github.dfauth.partial.Extractable._case;
import static com.github.dfauth.partial.Extractable._otherwise;
import static com.github.dfauth.partial.Matcher.match;
import static java.util.Objects.requireNonNull;

public class RootActorContext<T> implements ParentContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(RootActorContext.class);

    private final String name;
    private Behavior<T> guardianBehavior;
    private Map<String, DelegatingActorContext<?,T>> children = new HashMap<>();
    private Publisher publisher;
    private String topic;

    public RootActorContext(String topic, String name, Behavior.Factory<T> guardianBehaviorFactory, Publisher publisher) {
        this.topic = topic;
        this.name = requireNonNull(name);
        this.guardianBehavior = guardianBehaviorFactory.withActorContext(actorContext());
        this.publisher = publisher;
    }

    @Override
    public Optional<String> getId() {
        return Optional.ofNullable(name);
    }

    @Override
    public <R,S> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<S,?>> optSender) {
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
    public void processMessage(String address, Envelope<T> e) {
        List<String> l = Stream.of(address.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList());
        match(segment(l)).using(
                _case((h, t) -> h.equals(name) && t.isEmpty(),
                      (h, t) -> {
                        guardianBehavior = guardianBehavior.onMessage(e);
                      }),
                _case((h, t) -> h.equals(name),
                      (h, t) -> {
                        descend(t);
                      }),
                _otherwise(
                      (h, t) -> {
                          logger.error("unable to match actor address {}, {}",h,t);
                      })
        );
    }

    private Optional<ActorRef<T>> descend(List<String> l) {
        return match(segment(l)).using(
                _case((h, t) -> children.keySet().contains(h) && t.isEmpty(),
                        (h, t) ->
                            (ActorRef<T>) children.get(h).getActorRef()
                        ),
                _case((String h, List<String> t) -> children.keySet().contains(h),
                      (String h, List<String> t) -> descend(t).orElseThrow()
                )
        );
    }

    @Override
    public String getTopic() {
        return topic;
    }
}
