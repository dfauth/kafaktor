package com.github.dfauth.actor.kafka.guice;

import com.github.dfauth.actor.kafka.guice.providers.ConfigProvider;
import com.github.dfauth.actor.kafka.guice.providers.SchemaRegistryClientProvider;
import com.google.inject.AbstractModule;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProdModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(ProdModule.class);

    @Override
    protected void configure() {
        bind(SchemaRegistryClient.class).toProvider(SchemaRegistryClientProvider.class).asEagerSingleton();
        bind(Config.class).toProvider(ConfigProvider.class).asEagerSingleton();
    }
}
