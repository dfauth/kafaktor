package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.TestModule;
import com.github.dfauth.actor.kafka.test.GreetingRequest;
import com.github.dfauth.kafka.Stream;
import com.google.inject.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;


public class EnvelopeConsumerActorTestCase implements Consumer<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeConsumerActorTestCase.class);
    private static final String TOPIC = "topic";

    @Inject private Stream.Builder<String, byte[]> streamBuilder;
    @Inject private EnvelopeHandlerImpl<SpecificRecordBase> envelopeHandler;

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
            Injector injector = Guice.createInjector(MyModules.get());
            injector.injectMembers(this);

            ActorMessage env = envelopeHandler.envelope(actorRef, msg);
            Stream<String, byte[]> stream = streamBuilder.build(b -> b
                .withGroupId(this.getClass().getCanonicalName())
                .withKeyFilter(n -> n.equals(this.getClass().getCanonicalName()))
//                    .withValueDeserializer(envelopeHandler.envelopeDeserializer())
                .withMessageConsumer(this)
            );
            Thread.sleep(2 * 1000);
            stream.send(TOPIC, env.getKey(), envelopeHandler.envelopeSerializer().serialize(TOPIC, env));
            ActorMessage greeting = envelopeHandler.envelope("fred", GreetingRequest.newBuilder().setName("Fred").build());
            stream.send(TOPIC, greeting.getKey(), envelopeHandler.envelopeSerializer().serialize(TOPIC, greeting));
            Thread.sleep(10 * 1000);
        }));

    }

    private static final String CONFIG = "{\n" +
            "  kafka {\n" +
            "    topic: %s\n" +
            "    schema.registry.url: \"http://localhost:8080\"\n" +
            "    schema.registry.autoRegisterSchema: true\n" +
            "  }\n" +
            "}";

    @Override
    public void accept(byte[] bytes) {

    }
}

