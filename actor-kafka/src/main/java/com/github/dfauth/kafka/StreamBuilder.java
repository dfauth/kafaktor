package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.*;

public class StreamBuilder<K,V> {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);
    private static ExecutorService DEFAULT_EXECUTOR;

    private Duration maxOffsetCommitInterval = Duration.ofMillis(5000);
    private Duration pollingDuration = Duration.ofMillis(100);
    private Function<ConsumerRecord<K, V>, Long> recordProcessingFunction = record -> {
        logger.info("received record {} -> {} on topic: {}, partition: {}, offset: {}",record.key(), record.value(),record.topic(), record.partition(), record.offset());
        return record.offset() + 1;
    };
    private ExecutorService executor;
    private Collection<String> topics;
    private Map<String, Object> config;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private ConsumerAssignmentListener<K,V> topicPartitionConsumer = c -> tp -> {};

    public static <V> StreamBuilder<String,V> stringKeyBuilder() {
        return new StreamBuilder<String, V>()
                .withKeySerde(Serdes.String());
    }

    public static StreamBuilder<String,String> stringBuilder() {
        return new StreamBuilder<String, String>()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());
    }

    public static <K,V> StreamBuilder<K,V> builder() {
        return new StreamBuilder<>();
    }

    private static ExecutorService getDefaultExecutor() {
        if(DEFAULT_EXECUTOR == null) {
            synchronized (StreamBuilder.class) {
                if(DEFAULT_EXECUTOR == null) {
                    DEFAULT_EXECUTOR = Executors.newFixedThreadPool(4, r -> new Thread(null, r, StreamBuilder.class.getSimpleName()+"-defaultExecutor"));
                }
            }
        }
        return DEFAULT_EXECUTOR;
    }

    public StreamBuilder<K, V> withKeySerde(Serde<K> serde) {
        keySerde = serde;
        return this;
    }

    public StreamBuilder<K, V> withValueSerde(Serde<V> serde) {
        valueSerde = serde;
        return this;
    }

    public StreamBuilder<K, V> withKeyDeserializer(Deserializer<K> deserializer) {
        return withKeySerde(new Serde<K>() {
            @Override
            public Serializer<K> serializer() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Deserializer<K> deserializer() {
                return deserializer;
            }
        });
    }

    public StreamBuilder<K, V> withValueDeserializer(Deserializer<V> deserializer) {
        return withValueSerde(new Serde<V>() {
            @Override
            public Serializer<V> serializer() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Deserializer<V> deserializer() {
                return deserializer;
            }
        });
    }

    public StreamBuilder<K, V> withKeySerializer(Serializer<K> serializer) {
        return withKeySerde(new Serde<K>() {
            @Override
            public Serializer<K> serializer() {
                return serializer;
            }

            @Override
            public Deserializer<K> deserializer() {
                throw new UnsupportedOperationException();
            }
        });
    }

    public StreamBuilder<K, V> withValueSerializer(Serializer<V> serializer) {
        return withValueSerde(new Serde<V>() {
            @Override
            public Serializer<V> serializer() {
                return serializer;
            }

            @Override
            public Deserializer<V> deserializer() {
                throw new UnsupportedOperationException();
            }
        });
    }

    public StreamBuilder<K, V> withAssignmentConsumer(ConsumerAssignmentListener<K,V> c) {
        topicPartitionConsumer = c;
        return this;
    }

    public StreamBuilder<K, V> withMessageConsumer(Consumer<V> c) {
        return withRecordConsumer((k,v) -> c.accept(v));
    }

    public StreamBuilder<K, V> withRecordConsumer(BiConsumer<K, V> c) {
        return withRecordConsumer(r -> c.accept(r.key(), r.value()));
    }

    public StreamBuilder<K, V> withRecordConsumer(Consumer<ConsumerRecord<K,V>> recordConsumer) {
        return withRecordProcessor(r -> tryCatch(() -> {
            recordConsumer.accept(r);
            return r.offset() + 1;
        }, e -> r.offset()+1));
    }

    public StreamBuilder<K, V> withRecordProcessor(Function<ConsumerRecord<K,V>,Long> recordProcessor) {
        this.recordProcessingFunction = recordProcessor;
        return this;
    }

    public StreamBuilder<K, V> withProperties(Map<String, Object> config) {
        this.config = config;
        return this;
    }

    public StreamBuilder<K, V> withExecutor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public StreamBuilder<K, V> withTopic(String topic) {
        return withTopics(Collections.singletonList(topic));
    }

    public StreamBuilder<K, V> withTopics(Collection<String> topics) {
        this.topics = topics;
        return this;
    }

    public StreamBuilder<K, V> withPollingDuration(Duration duration) {
        this.pollingDuration = duration;
        return this;
    }

    public StreamBuilder<K, V> withOffsetCommitInterval(Duration duration) {
        this.maxOffsetCommitInterval = duration;
        return this;
    }

    public Stream build() {
        ExecutorService executor = this.executor != null ? this.executor : getDefaultExecutor();
        return new MultithreadedKafkaConsumer<>(config, topics, keySerde, valueSerde, executor, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer);
    }

    static class MultithreadedKafkaConsumer<K,V> implements Runnable, ConsumerRebalanceListener, Stream<K,V> {

        private final KafkaConsumer<K,V> consumer;
        private final KafkaProducer<K,V> producer;
        private final ExecutorService executor;
        private final Map<TopicPartition, Task<K,V>> activeTasks = new HashMap<>();
        private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final Collection<String> topics;
        private Instant lastCommitTime = Instant.now();
        private final Function<ConsumerRecord<K,V>, Long> recordProcessingFunction;
        private Duration pollingDuration;
        private Duration maxOffsetCommitInterval;
        private ConsumerAssignmentListener<K,V> topicPartitionConsumer;

        MultithreadedKafkaConsumer(Map<String, Object> config, Collection<String> topics, Serde<K> keySerde, Serde<V> valueSerde, ExecutorService executor, Function<ConsumerRecord<K, V>, Long> recordProcessingFunction, Duration pollingDuration, Duration maxOffsetCommitInterval, ConsumerAssignmentListener<K,V> topicPartitionConsumer) {
            this.topics = topics;
            this.executor = executor;
            this.recordProcessingFunction = recordProcessingFunction;
            this.pollingDuration = pollingDuration;
            this.maxOffsetCommitInterval = maxOffsetCommitInterval;
            this.topicPartitionConsumer = topicPartitionConsumer;
            Map<String, Object> props = new HashMap<>(config);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.computeIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ignored -> "earliest");
            consumer = new KafkaConsumer<>(props, keySerde.deserializer(), valueSerde.deserializer());
            producer = new KafkaProducer<>(props, keySerde.serializer(), valueSerde.serializer());
        }

        public void start() {
            consumer.subscribe(topics, this);
            new Thread(null, this, this.getClass().getSimpleName()+"-pollingThread-"+topics).start();
        }

        @Override
        public void run() {
            try {
                while (!stopped.get()) {
                    processRecords(consumer.poll(pollingDuration));
                    checkActiveTasks();
                    commitOffsets();
                }
            } catch (WakeupException we) {
                if (!stopped.get())
                    throw we;
            } finally {
                consumer.close();
            }
        }


        private void processRecords(ConsumerRecords<K,V> records) {
            if (records.count() > 0) {
                List<TopicPartition> partitionsToPause = new ArrayList<>();
                records.partitions().forEach(partition -> {
                    List<ConsumerRecord<K,V>> partitionRecords = records.records(partition);
                    Task<K,V> task = new Task(partitionRecords, recordProcessingFunction);
                    partitionsToPause.add(partition);
                    executor.submit(task);
                    activeTasks.put(partition, task);
                });
                consumer.pause(partitionsToPause);
            }
        }

        private void commitOffsets() {
            try {
                Instant now = Instant.now();
                if (lastCommitTime.plus(maxOffsetCommitInterval).isBefore(now)) {
                    if(!offsetsToCommit.isEmpty()) {
                        consumer.commitSync(offsetsToCommit);
                        offsetsToCommit.clear();
                    }
                    lastCommitTime = now;
                }
            } catch (RuntimeException e) {
                logger.error("Failed to commit offsets: "+e.getMessage(), e);
                throw e;
            }
        }


        private void checkActiveTasks() {
            List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
            activeTasks.forEach((partition, task) -> {
                if (task.isFinished()) {
                    finishedTasksPartitions.add(partition);
                }
                long offset = task.getCurrentOffset();
                if (offset > 0) {
                    offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
                }
            });
            finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
            consumer.resume(finishedTasksPartitions);
        }


        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            // 1. Stop all tasks handling records from revoked partitions
            Map<TopicPartition, Task<K,V>> stoppedTask = new HashMap<>();
            for (TopicPartition partition : partitions) {
                Task task = activeTasks.remove(partition);
                if (task != null) {
                    task.stop();
                    stoppedTask.put(partition, task);
                }
            }

            // 2. Wait for stopped tasks to complete processing of current record
            stoppedTask.forEach((partition, task) -> {
                long offset = task.waitForCompletion();
                if (offset > 0) {
                    offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
                }
            });


            // 3. collect offsets for revoked partitions
            Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
            partitions.forEach( partition -> {
                OffsetAndMetadata offset = offsetsToCommit.remove(partition);
                if (offset != null) {
                    revokedPartitionOffsets.put(partition, offset);
                }
            });

            // 4. commit offsets for revoked partitions
            try {
                consumer.commitSync(revokedPartitionOffsets);
            } catch (RuntimeException e) {
                logger.warn("Failed to commit offsets for revoked partitions: "+e.getMessage(), e);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            topicPartitionConsumer
                    .withKafkaConsumer(consumer)
                    .onAssignment(partitions);
            consumer.resume(partitions);
        }


        public void stop() {
            stopped.set(true);
            consumer.wakeup();
        }

        @Override
        public CompletableFuture<RecordMetadata> send(String topic, K k, V v) {
            CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
            producer.send(new ProducerRecord<>(topic,k,v), (m, e) -> {
                if(m != null && e == null) {
                    f.complete(m);
                } else if(m == null && e != null) {
                    f.completeExceptionally(e);
                } else {
                    // should not happen
                    logger.warn("producer received unhandled callback combination metadata: {}, exception: {}",m,e);
                }
            });
            return f;
        }

    }
}