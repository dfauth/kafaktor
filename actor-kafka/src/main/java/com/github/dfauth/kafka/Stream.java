package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.CompletableFuture;

interface Stream<K,V> {
    void start();
    void stop();
    CompletableFuture<RecordMetadata> send(String topic, K k, V v);
    default CompletableFuture<RecordMetadata> send(String topic, V v) {
        return send(topic, null, v);
    }
}
