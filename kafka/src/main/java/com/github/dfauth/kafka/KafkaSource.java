package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public interface KafkaSource {

    void start();
    void stop();

    class Builder<K,V> extends KafkaStream.Builder<KafkaSource.Builder<K,V>,K,V> {

        protected Collection<String> topics;

        public static Builder<String,String> builder() {
            return builder(Serdes.String(), Serdes.String());
        }

        public static <V> Builder<String,V> stringKeyBuilder(Serde<V> valueSerde) {
            return builder(Serdes.String(), valueSerde);
        }

        public static <K,V> Builder<K,V> builder(Serde<K> keySerde, Serde<V> valueSerde) {
            return new Builder<>(keySerde, valueSerde);
        }

        private Duration maxOffsetCommitInterval = Duration.ofMillis(5000);
        private Duration pollingDuration = Duration.ofMillis(100);
        private Function<ConsumerRecord<K, V>, Long> recordProcessingFunction = record -> {
            logger.info("received record {} -> {} on topic: {}, partition: {}, offset: {}",record.key(), record.value(),record.topic(), record.partition(), record.offset());
            return record.offset() + 1;
        };
        private ExecutorService executor;
        private PartitionAssignmentEventConsumer<K,V> topicPartitionConsumer = c -> tp -> {};
        private Predicate<ConsumerRecord<K, V>> predicate = r -> true;

        public Builder(Serde<K> keySerde, Serde<V> valueSerde) {
            super(keySerde, valueSerde);
        }

        public Builder<K, V> withTopic(String topic) {
            return withTopics(Collections.singletonList(topic));
        }

        public Builder<K, V> withTopics(Collection<String> topics) {
            this.topics = topics;
            return this;
        }

        public Builder<K, V> withPartitionAssignmentEventConsumer(PartitionAssignmentEventConsumer<K,V> c) {
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

        public Builder<K, V> withExecutor(ExecutorService executor) {
            this.executor = executor;
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

        public KafkaSource build() {
            return executor != null ?
                    new MultithreadedKafkaConsumer<>(config, topics, keySerde, valueSerde, executor, predicate, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer) :
                    new SimpleKafkaConsumer<>(config, topics, keySerde, valueSerde, predicate, recordProcessingFunction, pollingDuration, maxOffsetCommitInterval, topicPartitionConsumer);
        }

    }
}
