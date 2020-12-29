package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnveloperHandlerImpl;
import com.github.dfauth.actor.kafka.confluent.AvroDeserializer;
import com.github.dfauth.actor.kafka.confluent.AvroSerializer;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.kafka.StreamBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;


public class CreateActorTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CreateActorTestCase.class);
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";

    private SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    private AvroSerializer<ActorMessage> serializer = AvroSerializer.<ActorMessage>builder().withSchemaRegistryClient(schemaRegistryClient).build();
    private AvroDeserializer<ActorMessage> deserializer = AvroDeserializer.<ActorMessage>builder().withSchemaRegistryClient(schemaRegistryClient).build();
    private EnveloperHandlerImpl envelopeHandler = new EnveloperHandlerImpl(Serdes.serdeFrom(serializer, deserializer));

    @Test
    public void testCreateActor() throws IOException {

        String actorRef = "bootstrap";

        BehaviorFactoryEvent msg = BehaviorFactoryEvent.newBuilder().setImplementationClassName(TestActorCreationFunction.class.getCanonicalName()).setName("fred").build();
        ActorMessage env = envelopeHandler.envelope(actorRef, msg);

        Config config = ConfigFactory.parseString(String.format("{kafka.topic: %s}", TOPIC));
        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {
            Stream<String, ActorMessage> stream = createStream(p);
            new BootstrapActor(p.entrySet().stream().reduce(config,
                    (c, e) -> c.withValue("kafka." + e.getKey(), ConfigValueFactory.fromAnyRef(e.getValue())),
                    (c1, c2) -> c1.withFallback(c2)
            ), deserializer).start();
            stream.start();
            Thread.sleep(2 * 1000);
            stream.send(TOPIC, env.getKey(), env);
            Thread.sleep(10 * 1000);
        }));

    }

    private Stream createStream(Map<String, Object> p) {
        p.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getCanonicalName());
        return StreamBuilder.<String, ActorMessage>builder()
                .withProperties(p)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.serdeFrom(serializer, deserializer))
                .withTopic(TOPIC)
                .build();
    }

}
