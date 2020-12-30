package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.EnvelopeConsumer;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.confluent.AvroDeserializer;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvelopeConsumerTestActor implements EnvelopeConsumer<ActorMessage> {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeConsumerTestActor.class);

    private final Config config;
    private final AvroDeserializer<SpecificRecord> deserializer;

    public EnvelopeConsumerTestActor(Config config) {
        this.config = config;
        String url = config.getString("kafka.schema.registry.url");
        deserializer = AvroDeserializer.builder().withUrl(url).withSchemaRegistryClientFactory(u -> new CachedSchemaRegistryClient(u, 1024)).build();
    }

    @Override
    public void receive(Envelope<ActorMessage> e) {
        logger.info("onMessage({})", e.payload());
        SpecificRecord record = e.payload().mapPayload(deserializer);
    }

}
