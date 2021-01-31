package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import org.apache.kafka.clients.producer.RecordMetadata;

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

    default <R,S> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<S,?>> optSender) {
        // either provide a parental context to delagate to
        // or implement publishing
        return getParentContext()
                .map(c -> c.publish(recipient, msg, optSender))
                .orElse(CompletableFuture.failedFuture(new IllegalStateException("No implementation of publish provided")));
    }

    default <R> Optional<ParentContext<R>> getParentContext() {
        return Optional.empty();
    }

    boolean stop();

    void processMessage(ActorKey actorKey, Envelope<T> apply);

    default String getTopic() {
        return getParentContext().map(c -> c.getTopic()).orElseThrow(() -> new IllegalStateException("No parent available to delegate to"));
    }
}
