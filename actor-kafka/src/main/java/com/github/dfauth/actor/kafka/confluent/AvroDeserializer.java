package com.github.dfauth.actor.kafka.confluent;

import com.github.dfauth.actor.kafka.DeserializingFunction;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Map;
import java.util.function.Function;

public class AvroDeserializer<T extends SpecificRecordBase> implements DeserializingFunction<T> {

    private final KafkaAvroDeserializer deserializer;

    public static <T extends SpecificRecordBase> Builder<T> builder() {
        return new Builder();
    }

    public AvroDeserializer(String url, boolean isAutoRegisterSchemas, Function<String, SchemaRegistryClient> f, boolean isKey) {
        deserializer = new KafkaAvroDeserializer(f.apply(url));
        deserializer.configure(Map.of("schema.registry.url", url, "specific.avro.reader", true, "auto.register.schemas", isAutoRegisterSchemas), isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return (T) deserializer.deserialize(topic, data);
    }

    public static class Builder<T extends SpecificRecordBase> {
        private String url = "none";
        private boolean isAutoRegisterSchemas = true;
        private Function<String, SchemaRegistryClient> schemaRegistryClientFactory = url -> new CachedSchemaRegistryClient(url, 1024);
        private boolean isKeyDeserializer = false;

        public AvroDeserializer<T> build() {
            return new AvroDeserializer<>(url, isAutoRegisterSchemas, schemaRegistryClientFactory, isKeyDeserializer);
        }

        public Builder<T> withAutoRegisterSchema(boolean isAutoRegisterSchemas) {
            this.isAutoRegisterSchemas = isAutoRegisterSchemas;
            return this;
        }

        public Builder<T> withIsKeyDeserializer(boolean isKeyDeserializer) {
            this.isKeyDeserializer = isKeyDeserializer;
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
