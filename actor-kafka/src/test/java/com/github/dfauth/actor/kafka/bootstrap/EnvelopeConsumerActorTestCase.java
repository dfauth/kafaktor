package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.TestModule;
import com.github.dfauth.actor.kafka.test.GreetingRequest;
import com.github.dfauth.kafka.Stream;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static com.github.dfauth.utils.ConfigBuilder.builder;
import static com.github.dfauth.utils.ConfigBuilder.builderFrom;


public class EnvelopeConsumerActorTestCase implements Consumer<ActorMessage> {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeConsumerActorTestCase.class);
    private static final String TOPIC = "topic";

    @Inject private Stream.Builder<String, byte[]> streamBuilder;
    @Inject private EnvelopeHandlerImpl<SpecificRecordBase> envelopeHandler;

    @Test
    public void testIt() {

        String actorRef = "bootstrap";

        MyModules.register(new CommonModule(), new TestModule());

        EnvelopeConsumerEvent msg = EnvelopeConsumerEvent.newBuilder().setImplementationClassName(EnvelopeConsumerTestActor.class.getCanonicalName()).setName("fred").build();

        Config config = builder()
                .node("kafka")
                .value("topic", TOPIC)
                .node("schema")
                .node("registry")
                .value("url", "http://localhost:8080")
                .value("autoRegisterSchema", true).build();

        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {
            Config x = builderFrom(config).node("kafka").values(p).build();

            MyModules.register(binder -> binder.bind(Config.class).toInstance(x));
            Injector injector = Guice.createInjector(MyModules.get());
            injector.injectMembers(this);

            ActorMessage env = envelopeHandler.envelope(actorRef, msg);
            Stream<String, ActorMessage> stream = streamBuilder.build(b -> b
                .withGroupId(this.getClass().getCanonicalName())
                .withKeyFilter(n -> n.equals(this.getClass().getCanonicalName()))
                .withValueSerde(envelopeHandler.envelopeSerde())
                .withMessageConsumer(this)
            );
            Thread.sleep(2 * 1000);
            stream.send(TOPIC, env.getKey(), env);
            ActorMessage greeting = envelopeHandler.envelope("fred", GreetingRequest.newBuilder().setName("Fred").build());
            stream.send(TOPIC, greeting.getKey(), greeting);
            Thread.sleep(10 * 1000);
        }));

    }

    @Override
    public void accept(ActorMessage message) {

    }
}

