package com.github.dfauth.akka;

import akka.actor.typed.Behavior;
import com.github.dfauth.kafka.Stream;
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

public class AkkaTypedTest implements Consumer<String> {

    private static final Logger logger = LoggerFactory.getLogger(AkkaTypedTest.class);
    private static final String TOPIC = "topic";
    private ExecutorService executor = Executors.newCachedThreadPool();

    @Test
    public void testIt() {

        embeddedKafkaWithTopic(TOPIC).runTestConsumer(p -> tryCatch(() -> {

            Behavior<HelloWorldMain.SayHello> behavior = HelloWorldMain.create();

            Stream<String, byte[]> stream = Stream.Builder.stringKeyBuilder(Serdes.ByteArray())
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withGroupId(this.getClass().getCanonicalName())
//                    .withKeyFilter(n -> n.equals(this.getClass().getCanonicalName()))
                    .withMessageConsumer(bytes -> this.accept(new String(bytes)))
                    .withExecutor(executor)
                    .withPartitionAssignmentEventConsumer(c -> e -> {
                        e.onAssigment(_p -> {
                            logger.info("partitions assigned: "+_p);
                            Map<TopicPartition, Long> bo = c.beginningOffsets(_p);
                            Map<TopicPartition, Long> eo = c.endOffsets(_p);
                            _p.forEach(__p -> {
                                logger.info("partition: {} offsets beginning: {} current: {} end: {}",__p,bo.get(__p), c.position(__p),eo.get(__p));
                                Bootstrapper<HelloWorldMain.SayHello> bootstrapper = new Bootstrapper(__p, behavior, RecoveryStrategyEnum.TIME_BASED);
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
    public void accept(String msg) {
        logger.info("WOOZ received bytes: {}", msg);
    }
}
