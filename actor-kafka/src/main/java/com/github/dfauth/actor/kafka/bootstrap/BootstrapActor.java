package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.kafka.StreamBuilder;
import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.github.dfauth.utils.ConfigUtils.wrap;


public class BootstrapActor implements Consumer<ConsumerRecord<String, byte[]>>, DespatchableHandler {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapActor.class);

    private static final String GROUP_ID = "groupId";
    private final SchemaRegistryClient schemaRegistryClient;
    private final KafkaAvroDeserializer deserializer;
    private ConfigUtils config;
    private Stream stream;
    private String name;

    public BootstrapActor(Config config, SchemaRegistryClient schemaRegistryClient) {
        this.config = wrap(config);
        this.schemaRegistryClient = schemaRegistryClient;
        this.name = this.config.getString("actor.bootstrap").orElse("bootstrap");
        this.deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        this.deserializer.configure(Map.of("schema.registry.url", "dummy", "specific.avro.reader", true, "auto.register.schemas", true), false);
    }

    public void start() {
        Map<String, Object> p = config.map(c -> c.getConfig("kafka").entrySet().stream().reduce(new HashMap(), (acc, e) -> {
            acc.put(e.getKey(), e.getValue().unwrapped());
            return acc;
        }, (acc1, acc2) -> {
            acc1.putAll(acc2);
            return acc1;
        }));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        this.stream = StreamBuilder.<String, byte[]>builder()
                .withProperties(p)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()))
                .withTopic(config.map(c -> c.getString("kafka.topic")))
                .withKeyFilter(
                        k -> name.equals(k)
                )
                .withRecordConsumer(this)
                .build();
        stream.start();
    }

    public void stop() {
        stream.stop();
    }

    @Override
    public void accept(ConsumerRecord<String, byte[]> record) {
        logger.info("received record: {}",record);
        ActorMessage actorMessage = (ActorMessage) deserializer.deserialize(record.topic(), record.value());
        logger.info("received actor message: {}",actorMessage);
        Despatchable payload = (Despatchable) actorMessage.mapPayload((topic, data) -> deserializer.deserialize(topic, data));
        payload.despatch(this);
    }

    @Override
    public void handle(ConfigFunctionEventDespatchable configFunction) {
        logger.info("received payload message: {}",configFunction);
        ActorRef actorRef = configFunction.apply(config.nested());
        logger.info("created actor ref: {}",actorRef);
    }
}
