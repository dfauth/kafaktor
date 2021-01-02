package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import javax.inject.Inject;
import javax.inject.Provider;


public class SchemaRegistryClientProvider implements Provider<SchemaRegistryClient> {

    private final SchemaRegistryClient schemaRegistryClient;

    @Inject
    public SchemaRegistryClientProvider(Config config) {
        ConfigUtils _config = ConfigUtils.wrap(config);
        String url = _config.getString("kafka.schema.registry.url").orElse("http://localhost:8080");
        int capacity = _config.getInt("kafka.schema.registry.capacity").orElse(1024);
        this.schemaRegistryClient = new CachedSchemaRegistryClient(url, capacity);
    }

    @Override
    public SchemaRegistryClient get() {
        return schemaRegistryClient;
    }
}
