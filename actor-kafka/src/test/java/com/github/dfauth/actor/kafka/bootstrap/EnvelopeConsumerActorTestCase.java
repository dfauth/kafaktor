package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.TestModule;
import com.github.dfauth.actor.kafka.test.GreetingRequest;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.kafka.StreamBuilder;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;


public class EnvelopeConsumerActorTestCase implements Module {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeConsumerActorTestCase.class);
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";

//    private SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
//    private AvroSerializer<ActorMessage> serializer = AvroSerializer.<ActorMessage>builder().withSchemaRegistryClient(schemaRegistryClient).build();
//    private AvroDeserializer<ActorMessage> deserializer = AvroDeserializer.<ActorMessage>builder().withSchemaRegistryClient(schemaRegistryClient).build();
//    private EnvelopeHandlerImpl envelopeHandler = new EnvelopeHandlerImpl(Serdes.serdeFrom(serializer, deserializer));
    @Inject
    private StreamBuilder<String, String> streamBuilder;

    @Test
    public void testIt() {

        String actorRef = "bootstrap";

        MyModules.register(new CommonModule(), new TestModule());

        EnvelopeConsumerEvent msg = EnvelopeConsumerEvent.newBuilder().setImplementationClassName(EnvelopeConsumerTestActor.class.getCanonicalName()).setName("fred").build();

        Config config = ConfigFactory.parseString(String.format(CONFIG, TOPIC));
        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {
            Config x = p.entrySet().stream().reduce(config,
                    (c, e) -> c.withValue("kafka." + e.getKey(), ConfigValueFactory.fromAnyRef(e.getValue())),
                    (c1, c2) -> c1.withFallback(c2)
            );
            MyModules.register(binder -> binder.bind(Config.class).toInstance(x));

            EnvelopeHandlerImpl envelopeHandler = injector().getInstance(EnvelopeHandlerImpl.class);
            ActorMessage env = envelopeHandler.envelope(actorRef, msg);
            new BootstrapActor(x, envelopeHandler.envelopeDeserializer()).start();
            Stream<String, byte[]> stream = (Stream<String, byte[]>) injector().getInstance(StreamBuilder.class).build(); //createStream(p);
//            StreamBuilder<String, ActorMessage> builder = (StreamBuilder<String, ActorMessage>) injector().getInstance(StreamBuilder.class); //createStream(p);
//            Stream stream = builder.withTopic(TOPIC).build();
//            stream.start();
            Thread.sleep(2 * 1000);
            stream.send(TOPIC, env.getKey(), envelopeHandler.envelopeSerializer().serialize(TOPIC, env));
            ActorMessage greeting = envelopeHandler.envelope("fred", GreetingRequest.newBuilder().setName("Fred").build());
            stream.send(TOPIC, greeting.getKey(), envelopeHandler.envelopeSerializer().serialize(TOPIC, greeting));
            Thread.sleep(10 * 1000);
        }));

    }

    private Injector injector() {
        return Guice.createInjector(MyModules.get());
    }

    private Stream createStream(Map<String, Object> p) {
//        p.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getCanonicalName());
//        return StreamBuilder.<String, ActorMessage>builder()
//                .withProperties(p)
//                .withKeySerde(Serdes.String())
//                .withValueSerde(Serdes.serdeFrom(serializer, deserializer))
//                .withTopic(TOPIC)
//                .build();
        return this.streamBuilder
//                .withValueSerializer()
                .build();
    }

    private static final String CONFIG = "{\n" +
            "  kafka {\n" +
            "    topic: %s\n" +
            "    schema.registry.url: \"http://localhost:8080\"\n" +
            "    schema.registry.autoRegisterSchema: true\n" +
            "  }\n" +
            "}";

    @Override
    public void configure(Binder binder) {
        logger.info("binder: {}", binder);
        binder.requestInjection(this);
    }
}

