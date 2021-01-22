package com.github.dfauth.kafaktor.bootstrap;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface Publisher {
    <R, T> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<T,?>> optSender);
}
