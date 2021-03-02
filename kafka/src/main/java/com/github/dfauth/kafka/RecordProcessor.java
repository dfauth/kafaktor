package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface RecordProcessor<K, V> extends Function<ConsumerRecord<K, V>, CompletableFuture<Long>> {
}
