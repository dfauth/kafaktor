package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.actor.kafka.EnvelopeHandler;
import com.github.dfauth.actor.kafka.confluent.AvroDeserializer;
import com.github.dfauth.actor.kafka.confluent.AvroSerializer;
import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;

import javax.inject.Inject;
import javax.inject.Provider;


public class EnvelopeHandlerProvider implements Provider<EnvelopeHandler<SpecificRecordBase>> {

    private final EnvelopeHandler<SpecificRecordBase> envelopeHandler;

    @Inject
    public EnvelopeHandlerProvider(Config config, SchemaRegistryClient schemaRegistryClient) {
        ConfigUtils _config = ConfigUtils.wrap(config);
        boolean isAutoRegisterSchema = _config.getBoolean("kafka.schema.registry.autoRegisterSchema").orElse(false);
        this.envelopeHandler = EnvelopeHandler.of(Serdes.serdeFrom(
                AvroSerializer.builder().withAutoRegisterSchema(isAutoRegisterSchema).withSchemaRegistryClient(schemaRegistryClient).build(),
                AvroDeserializer.builder().withAutoRegisterSchema(isAutoRegisterSchema).withSchemaRegistryClient(schemaRegistryClient).build()
        ));
    }

    @Override
    public EnvelopeHandler<SpecificRecordBase> get() {
        return envelopeHandler;
    }
}
