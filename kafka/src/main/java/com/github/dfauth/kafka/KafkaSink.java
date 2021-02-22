package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.reactivestreams.Processor;

import java.util.Collections;
import java.util.Map;

import static com.github.dfauth.utils.FunctionUtils.merge;

public interface KafkaSink<K,V> extends Processor<ProducerRecord<K,V>,RecordMetadata> {

    void start();

    void stop();

    String topic();

    default ProducerRecord<K,V> toProducerRecord(V v) {
        return toProducerRecord(null, v);
    }

    default ProducerRecord<K,V> toProducerRecord(K k, V v) {
        return new ProducerRecord<>(topic(), k, v);
    }

    class Builder<K,V> extends KafkaStream.Builder<KafkaSink.Builder<K,V>,K,V> {

        protected String sinkTopic;
        private KafkaProducer<K, V> p;
        private Map<String, Object> producerConfig = Collections.emptyMap();

        public Builder(Serde<K> keySerde, Serde<V> valueSerde) {
            super(keySerde, valueSerde);
        }

        public static Builder<String, String> builder() {
            return builder(Serdes.String(), Serdes.String());
        }

        public static <V> Builder<String, V> builder(Serde<V> valueSerde) {
            return new Builder(Serdes.String(), valueSerde);
        }

        public static <V, K> Builder<K, V> builder(Serde<K> keySerde, Serde<V> valueSerde) {
            return new Builder(keySerde, valueSerde);
        }

        public Builder<K,V> withProducerConfig(Map<String, Object> producerConfig) {
            this.producerConfig = producerConfig;
            return this;
        }

        public Builder<K, V> withSinkTopic(String topic) {
            this.sinkTopic = topic;
            return this;
        }

        public KafkaSink<K,V> build() {
            return new SubscriptionSink<>(sinkTopic, new KafkaProducer<K, V>(merge(config, producerConfig), keySerde.serializer(), valueSerde.serializer()));
        }
    }
}
