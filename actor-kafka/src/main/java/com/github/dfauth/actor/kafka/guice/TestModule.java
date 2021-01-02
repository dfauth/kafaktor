package com.github.dfauth.actor.kafka.guice;

import com.google.inject.AbstractModule;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(TestModule.class);
    private SchemaRegistryClient instance = new MockSchemaRegistryClient();

    @Override
    protected void configure() {
        bind(SchemaRegistryClient.class).toInstance(instance);
    }
}
