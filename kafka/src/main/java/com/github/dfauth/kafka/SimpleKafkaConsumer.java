package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;


class SimpleKafkaConsumer<K,V> implements Runnable, ConsumerRebalanceListener, KafkaSource, ConsumerRecordOps {

    private final Logger logger = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    private final KafkaConsumer<K,V> consumer;
    private final Map<TopicPartition, CompletableFuture<OffsetAndMetadata>> offsetsToCommit = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Collection<String> topics;
    private Instant lastCommitTime = Instant.now();
    private final Predicate<ConsumerRecord<K,V>> predicate;
    private final Function<ConsumerRecord<K,V>, CompletableFuture<Long>> recordProcessor;
    private Duration pollingDuration;
    private Duration maxOffsetCommitInterval;
    private PartitionAssignmentEventConsumer<K,V> partitionAssignmentEventConsumer;

    SimpleKafkaConsumer(Map<String, Object> config, Collection<String> topics, Serde<K> keySerde, Serde<V> valueSerde, Predicate<ConsumerRecord<K, V>> predicate, Function<ConsumerRecord<K, V>, CompletableFuture<Long>> recordProcessor, Duration pollingDuration, Duration maxOffsetCommitInterval, PartitionAssignmentEventConsumer<K,V> partitionAssignmentEventConsumer) {
        this.topics = topics;
        this.predicate = predicate;
        this.recordProcessor = recordProcessor;
        this.pollingDuration = pollingDuration;
        this.maxOffsetCommitInterval = maxOffsetCommitInterval;
        this.partitionAssignmentEventConsumer = partitionAssignmentEventConsumer;
        Map<String, Object> props = new HashMap<>(config);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.computeIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ignored -> "earliest");
        consumer = new KafkaConsumer<>(props, keySerde.deserializer(), valueSerde.deserializer());
    }

    public void start() {
        tryCatch(() -> consumer.subscribe(topics, this));
        new Thread(null, this, this.getClass().getSimpleName()+"-pollingThread-"+topics).start();
    }

    @Override
    public void run() {
        try {
            while (!stopped.get()) {
                ConsumerRecords<K, V> records = consumer.poll(pollingDuration);
                records.partitions().stream().forEach(tp ->
                    records.records(tp).stream()
                            .filter(predicate)
                            .map(recordProcessor)
                            .forEach(f ->
                        offsetsToCommit.compute(tp, (k,v) -> v == null ?
                                f.thenApply(o -> new OffsetAndMetadata(o)) :
                                f.thenCompose(o -> v.thenApply(_v -> new OffsetAndMetadata(o, _v.leaderEpoch(), _v.metadata())))
                    )
                ));
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            consumer.close();
        }
    }


    private void commitOffsets() {
        try {
            Instant now = Instant.now();
            if (lastCommitTime.plus(maxOffsetCommitInterval).isBefore(now)) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(filterCompletedOffsets(offsetsToCommit));
                    offsetsToCommit.clear();
                }
                lastCommitTime = now;
            }
        } catch (RuntimeException e) {
            logger.error("Failed to commit offsets: "+e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 3. collect offsets for revoked partitions
        Map<TopicPartition, CompletableFuture<OffsetAndMetadata>> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach( partition -> {
            CompletableFuture<OffsetAndMetadata> offset = offsetsToCommit.remove(partition);
            if (offset != null) {
                revokedPartitionOffsets.put(partition, offset);
            }
        });

        // 4. commit offsets for revoked partitions
        try {
            consumer.commitSync(filterCompletedOffsets(revokedPartitionOffsets));
        } catch (RuntimeException e) {
            logger.warn("Failed to commit offsets for revoked partitions: "+e.getMessage(), e);
        }

        partitionAssignmentEventConsumer
                .withKafkaConsumer(consumer)
                .onAssignment(new AssignmentListener.PartitionsRevokedEvent(partitions));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitionAssignmentEventConsumer
                .withKafkaConsumer(consumer)
                .onAssignment(new AssignmentListener.PartitionsAssignedEvent(partitions));
        consumer.resume(partitions);
    }


    public void stop() {
        stopped.set(true);
        consumer.wakeup();
    }

}
