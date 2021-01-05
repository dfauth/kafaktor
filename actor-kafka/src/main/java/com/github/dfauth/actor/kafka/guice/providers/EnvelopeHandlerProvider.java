package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.confluent.AvroDeserializer;
import com.github.dfauth.actor.kafka.confluent.AvroSerializer;
import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;

import javax.inject.Inject;
import javax.inject.Provider;


public class EnvelopeHandlerProvider implements Provider<EnvelopeHandlerImpl<SpecificRecordBase>> {

    private final EnvelopeHandlerImpl<SpecificRecordBase> envelopeHandler;

    @Inject
    public EnvelopeHandlerProvider(Config config, SchemaRegistryClient schemaRegistryClient) {
        ConfigUtils _config = ConfigUtils.wrap(config);
        boolean isAutoRegisterSchema = _config.getBoolean("kafka.schema.registry.autoRegisterSchema").orElse(false);
        this.envelopeHandler = new EnvelopeHandlerImpl(Serdes.serdeFrom(
                AvroSerializer.builder().withAutoRegisterSchema(isAutoRegisterSchema).withSchemaRegistryClient(schemaRegistryClient).build(),
                AvroDeserializer.builder().withAutoRegisterSchema(isAutoRegisterSchema).withSchemaRegistryClient(schemaRegistryClient).build()
        ));
    }

    @Override
    public EnvelopeHandlerImpl<SpecificRecordBase> get() {
        return envelopeHandler;
    }
}
