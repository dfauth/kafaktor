package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class StreamBuilder<K,V> {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

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
        return executor != null ?
                new MultithreadedKafkaConsumer<>(config, topics, keySerde, valueSerde, executor, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer) :
                new SimpleKafkaConsumer<>(config, topics, keySerde, valueSerde, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer);
    }

}