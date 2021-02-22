package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

public interface KafkaSink<K,V> {

    void start();

    void stop();

    String topic();

    default CompletableFuture<RecordMetadata> send(V v) {
        return send(topic(), null, v);
    }

    default CompletableFuture<RecordMetadata> send(K k, V v) {
        return send(topic(), k, v);
    }

    default CompletableFuture<RecordMetadata> send(String topic, K k, V v) {
        return send(new ProducerRecord<>(topic, k, v));
    }
    CompletableFuture<RecordMetadata> send(ProducerRecord<K,V> record);

    class Builder<K,V> extends KafkaStream.Builder<KafkaSink.Builder<K,V>,K,V> {

        protected String topic;
        private KafkaProducer<K, V> p;
        private Flow.Publisher<ProducerRecord<K, V>> publisher;

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

        public Builder<K, V> withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        @Override
        public KafkaSink.Builder<K, V> withPublisher(Flow.Publisher<ProducerRecord<K,V>> p) {
            this.publisher = p;
            return this;
        }

        public KafkaSink<K,V> build() {
            return new KafkaSink<K,V>() {
                @Override
                public void start() {
                    p = new KafkaProducer<>(config, keySerde.serializer(), valueSerde.serializer());
                }

                @Override
                public void stop() {
                    p.close();
                }

                @Override
                public String topic() {
                    return topic;
                }

                @Override
                public CompletableFuture<RecordMetadata> send(ProducerRecord<K,V> record) {
                    CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
                    p.send(record, (m, e) ->
                        Optional.ofNullable(m).map(_m -> f.complete(_m)).orElse(f.completeExceptionally(e))
                    );
                    return f;
                }
            };
        }
    }
}
