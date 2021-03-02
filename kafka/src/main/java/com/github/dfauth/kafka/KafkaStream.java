package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public interface KafkaStream {

    class Builder<B extends Builder<B,K,V>,K,V> {

        protected static final Logger logger = LoggerFactory.getLogger(Builder.class);

        protected Map<String, Object> config;
        protected Serde<K> keySerde;
        protected Serde<V> valueSerde;

        public Builder(Serde<K> keySerde, Serde<V> valueSerde) {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public B withConfig(Map<String, Object> config) {
            this.config = config;
            return (B) this;
        }

        public KafkaSource.Builder<K, V> withSourceTopic(String topic) {
            return KafkaSource.Builder
                    .builder(keySerde, valueSerde)
                    .withSourceTopic(topic)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K, V> withSourceTopics(Collection<String> topics) {
            return KafkaSource.Builder
                    .builder(keySerde, valueSerde)
                    .withSourceTopics(topics)
                    .withConfig(config);
        }

        public KafkaSink.Builder<K, V> withSinkTopic(String topic) {
            return KafkaSink.Builder
                    .builder(keySerde, valueSerde)
                    .withSinkTopic(topic)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K,V> withMessageConsumer(Consumer<V> consumer) {
            return KafkaSource.Builder.builder(keySerde, valueSerde)
                    .withMessageConsumer(consumer)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K,V> withRecordConsumer(Consumer<ConsumerRecord<K,V>> consumer) {
            return KafkaSource.Builder.builder(keySerde, valueSerde)
                    .withRecordConsumer(consumer)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K,V> withRecordConsumer(BiConsumer<K,V> consumer) {
            return KafkaSource.Builder.builder(keySerde, valueSerde)
                    .withRecordConsumer(consumer)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K,V> withRecordProcessor(Function<ConsumerRecord<K,V>, CompletableFuture<Long>> f) {
            return KafkaSource.Builder.builder(keySerde, valueSerde)
                    .withRecordProcessor(f)
                    .withConfig(config);
        }

        public KafkaSource.Builder<K,V> withConsumerConfig(Map<String, Object> consumerConfig) {
            return KafkaSource.Builder.builder(keySerde, valueSerde)
                    .withConfig(config)
                    .withConsumerConfig(consumerConfig);
        }

        public KafkaSink.Builder<K,V> withProducerConfig(Map<String, Object> producerConfig) {
            return KafkaSink.Builder.builder(keySerde, valueSerde)
                    .withConfig(config)
                    .withProducerConfig(producerConfig);
        }
    }
}
