package com.github.dfauth.akka;

import akka.actor.typed.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.TestModule;
import com.github.dfauth.kafka.RecoveryStrategies;
import com.github.dfauth.kafka.Stream;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.github.dfauth.bootstrap.Bootstrapper;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static com.github.dfauth.utils.ConfigBuilder.builder;
import static com.github.dfauth.utils.ConfigBuilder.builderFrom;

public class AkkaTypedTest implements Consumer<ActorMessage> {

    private static final Logger logger = LoggerFactory.getLogger(AkkaTypedTest.class);
    private static final String TOPIC = "topic";
    private ExecutorService executor = Executors.newCachedThreadPool();

    @Inject
    private EnvelopeHandlerImpl<SpecificRecordBase> envelopeHandler;

    @Test
    public void testIt() {

        MyModules.register(new CommonModule(), new TestModule());

        Config config = builder()
                .node("kafka")
                .value("topic", TOPIC)
                .node("schema")
                .node("registry")
                .value("url", "http://localhost:8080")
                .value("autoRegisterSchema", true).build();

        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {

            MyModules.register(binder -> binder.bind(Config.class).toInstance(builderFrom(config).node("kafka").values(p).build()));
            Injector injector = Guice.createInjector(MyModules.get());
            injector.injectMembers(this);

            Behavior<HelloWorldMain.SayHello> behavior = HelloWorldMain.create();

            Stream<String, byte[]> stream = Stream.Builder.stringKeyBuilder(envelopeHandler.envelopeSerde())
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withGroupId(this.getClass().getCanonicalName())
//                    .withKeyFilter(n -> n.equals(this.getClass().getCanonicalName()))
                    .withMessageConsumer(this)
                    .withExecutor(executor)
                    .withPartitionAssignmentEventConsumer(c -> e -> {
                        e.onAssigment(_p -> {
                            logger.info("partitions assigned: "+_p);
                            Map<TopicPartition, Long> bo = c.beginningOffsets(_p);
                            Map<TopicPartition, Long> eo = c.endOffsets(_p);
                            _p.forEach(__p -> {
                                logger.info("partition: {} offsets beginning: {} current: {} end: {}",__p,bo.get(__p), c.position(__p),eo.get(__p));
                                Bootstrapper1<HelloWorldMain.SayHello> bootstrapper = new Bootstrapper1(__p, behavior, RecoveryStrategies.<String, Envelope<HelloWorldMain.SayHello>>timeBased());
                                bootstrapper.getRecoveryStrategy().invoke(c, __p,() ->
                                    // start of day is 6am local time
                                    Instant.from(LocalDate.now().atTime(LocalTime.of(6,0)).atZone(ZoneId.systemDefault()))
                                );
                                bootstrapper.start();
                            });
                        });
                        e.onRevocation(_p -> {
                            logger.info("partitions revoked: "+_p);
                            _p.forEach(__p -> Bootstrapper.lookup(__p).ifPresent(b -> b.stop()));
                        });
                    })
                    .build();
            stream.start();
            Thread.sleep(5 * 1000);
            stream.send(TOPIC, "key", "value".getBytes());
            Thread.sleep(5 * 1000);
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
    public void accept(ActorMessage msg) {
        logger.info("WOOZ received bytes: {}", msg);
    }
}