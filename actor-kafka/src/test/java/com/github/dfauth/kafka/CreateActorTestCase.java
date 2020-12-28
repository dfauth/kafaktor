package com.github.dfauth.kafka;

import com.github.dfauth.actor.MessageConsumer;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.bootstrap.BootstrapActor;
import com.github.dfauth.actor.kafka.create.ConfigFunctionEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;


public class CreateActorTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CreateActorTestCase.class);
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";

    @Test
    public void testCreateActor() throws IOException {

        String actorRef = "fred";
        ConfigFunctionEvent msg = ConfigFunctionEvent.newBuilder().setImplementation(TestActorCreationFunction.class.getCanonicalName()).build();
        ActorMessage env = ActorMessage.newBuilder().setPayload(msg.toByteBuffer()).setPayloadSchema(msg.getSchema().getFullName()).setTimestamp(Instant.now().toEpochMilli()).setKey(actorRef).setMetadata(Collections.emptyMap()).build();

        Config config = ConfigFactory.parseString(String.format("{kafka.topic: %s}", TOPIC));
        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {
            Stream<String, ActorMessage> stream = createStream(p);
            new BootstrapActor(p.entrySet().stream().reduce(config,
                    (c, e) -> c.withValue("kafka." + e.getKey(), ConfigValueFactory.fromAnyRef(e.getValue())),
                    (c1, c2) -> c1.withFallback(c2)
            )).start();
            stream.start();
            Thread.sleep(2 * 1000);
            stream.send(TOPIC, env);
            Thread.sleep(10 * 1000);
        }));

    }

    private Stream<String, ActorMessage> createStream(Map<String, Object> p) {
        p.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getCanonicalName());
        KafkaAvroSerializer x = new KafkaAvroSerializer(new MockSchemaRegistryClient());
        return StreamBuilder.<String, ActorMessage>builder()
                .withProperties(p)
                .withKeySerde(Serdes.String())
                .withValueSerializer((topic, data) -> x.serialize(TOPIC, data))
                .withTopic(TOPIC)
                .build();
    }

    static class TestActorCreationFunction<T> implements Function<Config, MessageConsumer<T>> {

        @Override
        public MessageConsumer<T> apply(Config config) {
            return m -> logger.info("received message: {}",m);
        }
    }
}
