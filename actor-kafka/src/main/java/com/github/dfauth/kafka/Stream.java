package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface Stream<K,V> {
    void start();
    void stop();
    CompletableFuture<RecordMetadata> send(String topic, K k, V v);
    default CompletableFuture<RecordMetadata> send(String topic, V v) {
        return send(topic, null, v);
    }

    class Builder<K,V> {

        private static final Logger logger = LoggerFactory.getLogger(Builder.class);

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
        private Predicate<ConsumerRecord<K, V>> predicate = r -> true;

        public static <V> Builder<String,V> stringKeyBuilder() {
            return new Builder<String, V>()
                    .withKeySerde(Serdes.String());
        }

        public static Builder<String,String> stringBuilder() {
            return new Builder<String, String>()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String());
        }

        public static <K,V> Builder<K,V> builder() {
            return new Builder<>();
        }

        public Builder<K, V> withKeySerde(Serde<K> serde) {
            keySerde = serde;
            return this;
        }

        public Builder<K, V> withValueSerde(Serde<V> serde) {
            valueSerde = serde;
            return this;
        }

        public Builder<K, V> withKeyDeserializer(Deserializer<K> deserializer) {
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

        public Builder<K, V> withValueDeserializer(Deserializer<V> deserializer) {
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

        public Builder<K, V> withKeySerializer(Serializer<K> serializer) {
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

        public Builder<K, V> withValueSerializer(Serializer<V> serializer) {
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

        public Builder<K, V> withAssignmentConsumer(ConsumerAssignmentListener<K,V> c) {
            topicPartitionConsumer = c;
            return this;
        }

        public Builder<K, V> withMessageConsumer(Consumer<V> c) {
            return withRecordConsumer((k,v) -> c.accept(v));
        }

        public Builder<K, V> withRecordConsumer(BiConsumer<K, V> c) {
            return withRecordConsumer(r -> c.accept(r.key(), r.value()));
        }

        public Builder<K, V> withRecordConsumer(Consumer<ConsumerRecord<K,V>> recordConsumer) {
            return withRecordProcessor(r -> tryCatch(() -> {
                recordConsumer.accept(r);
                return r.offset() + 1;
            }, e -> r.offset()+1));
        }

        public Builder<K, V> withRecordProcessor(Function<ConsumerRecord<K,V>,Long> recordProcessor) {
            this.recordProcessingFunction = recordProcessor;
            return this;
        }

        public Builder<K, V> withProperties(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Builder<K, V> withExecutor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder<K, V> withTopic(String topic) {
            return withTopics(Collections.singletonList(topic));
        }

        public Builder<K, V> withTopics(Collection<String> topics) {
            this.topics = topics;
            return this;
        }

        public Builder<K, V> withPollingDuration(Duration duration) {
            this.pollingDuration = duration;
            return this;
        }

        public Builder<K, V> withOffsetCommitInterval(Duration duration) {
            this.maxOffsetCommitInterval = duration;
            return this;
        }

        public Builder<K, V> withFilter(Predicate<ConsumerRecord<K,V>> p) {
            this.predicate = p;
            return this;
        }

        public Builder<K, V> withKeyFilter(Predicate<K> p) {
            this.predicate = r -> p.test(r.key());
            return this;
        }

        public Builder<K, V> withValueFilter(Predicate<V> p) {
            this.predicate = r -> p.test(r.value());
            return this;
        }

        public Builder<K, V> withGroupId(String groupId) {
            this.config.compute(ConsumerConfig.GROUP_ID_CONFIG, (k, v) -> {
                if(v != null) {
                    logger.warn("overriding previous {} of {} with {}", k,v,groupId);
                }
                return groupId;
            });
            return this;
        }

        public Stream<K,V> build() {
            return executor != null ?
                    new MultithreadedKafkaConsumer<>(config, topics, keySerde, valueSerde, executor, predicate, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer) :
                    new SimpleKafkaConsumer<>(config, topics, keySerde, valueSerde, predicate, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer);
        }

        public Stream<K, V> build(Consumer<Builder<K, V>> consumer) {
            Builder<K,V> clone = new Builder<K,V>().withKeySerde(keySerde)
                    .withValueSerde(valueSerde)
                    .withAssignmentConsumer(topicPartitionConsumer)
                    .withFilter(predicate)
                    .withOffsetCommitInterval(maxOffsetCommitInterval)
                    .withPollingDuration(pollingDuration)
                    .withProperties(config)
                    .withRecordProcessor(recordProcessingFunction)
                    .withTopics(topics)
                    .withExecutor(executor);
            consumer.accept(clone);
            return clone.build();
        }
    }
}
