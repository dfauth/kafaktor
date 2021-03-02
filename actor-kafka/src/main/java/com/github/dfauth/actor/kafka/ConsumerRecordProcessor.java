package com.github.dfauth.actor.kafka;

import com.github.dfauth.kafka.RecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;

public interface ConsumerRecordProcessor<K, V> extends RecordProcessor<K, V> {
    @Override
    default CompletableFuture<Long> apply(ConsumerRecord<K, V> r) {
        return process(r).thenApply(o -> o.next());
    }

    CompletableFuture<Offset> process(ConsumerRecord<K, V> r);
}
