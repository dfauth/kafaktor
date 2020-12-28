package com.github.dfauth.actor.kafka.confluent;

import com.github.dfauth.actor.kafka.SerializingFunction;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;
import java.util.function.Function;

public class AvroSerializer<T extends SpecificRecord> implements SerializingFunction<T> {

    private final KafkaAvroSerializer serializer;

    public static <T extends SpecificRecord> Builder<T> builder() {
        return new Builder();
    }

    public AvroSerializer(String url, boolean isAutoRegisterSchemas, Function<String, SchemaRegistryClient> f, boolean isKey) {
        serializer = new KafkaAvroSerializer(f.apply(url));
        serializer.configure(Map.of("schema.registry.url", url, "specific.avro.reader", true, "auto.register.schemas", isAutoRegisterSchemas), isKey);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        return serializer.serialize(topic, data);
    }

    public static class Builder<T extends SpecificRecord> {
        private String url = "none";
        private boolean isAutoRegisterSchemas = true;
        private Function<String, SchemaRegistryClient> schemaRegistryClientFactory = url -> new CachedSchemaRegistryClient(url, 1024);
        private boolean isKeySerializer = false;

        public AvroSerializer<T> build() {
            return new AvroSerializer<>(url, isAutoRegisterSchemas, schemaRegistryClientFactory, isKeySerializer);
        }

        public Builder<T> withAutoRegisterSchema(boolean isAutoRegisterSchemas) {
            this.isAutoRegisterSchemas = isAutoRegisterSchemas;
            return this;
        }

        public Builder<T> withIsKeySerializer(boolean isKeySerializer) {
            this.isKeySerializer = isKeySerializer;
            return this;
        }

        public Builder<T> withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder<T> withSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient) {
            this.schemaRegistryClientFactory = ignored -> schemaRegistryClient;
            return this;
        }

        public Builder<T> withSchemaRegistryClientFactory(Function<String, SchemaRegistryClient> schemaRegistryClientFactory) {
            this.schemaRegistryClientFactory = schemaRegistryClientFactory;
            return this;
        }
    }
}
