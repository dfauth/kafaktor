package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.checkerframework.checker.units.qual.K;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ParentContext<T> {

    default Optional<String> getId() {
        return Optional.empty();
    }

    <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name);

    default <R> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg) {
        return publish(recipient, msg, Optional.empty());
    }

    default <R> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, KafkaActorRef<T,?> sender) {
        return publish(recipient, msg, Optional.ofNullable(sender));
    }

    default <R> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<T,?>> optSender) {
        // either provide a parental context to delagate to
        // or implement publishing
        return getParentContext()
                .map(c -> c.publish(recipient, msg, optSender))
                .orElse(CompletableFuture.failedFuture(new IllegalStateException("No implementation of publish provided")));
    }

    default Optional<ParentContext<T>> getParentContext() {
        return Optional.empty();
    }

    boolean stop();

    Optional<ParentContext<T>> findActor(K key, Class<T> expectedType);

    void onMessage(Envelope<T> apply);
}
