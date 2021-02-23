package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.utils.TimerUtils.timedExecution;
import static org.junit.jupiter.api.Assertions.*;

public class MultiPartitionTestCase {

    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionTestCase.class);

    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";
    private static final String MESSAGE = "Hello World!";

    private static final int sleeptimeMsec = 100;
    private static final int trials = 100;
    private static final int partitionCount = 2;
    private static final double tolerance = 1.2;
    private static final ExecutorService executor = Executors.newFixedThreadPool(partitionCount);

    @Test
    public void testIt() {

        CompletableFuture<Collection<TopicPartition>> tpFuture = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> rmFuture = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(trials);

        Duration d = embeddedKafkaWithTopic(TOPIC).withPartitions(partitionCount).runTest(p -> {
            KafkaSource.Builder<Integer, Integer> builder = KafkaSource.Builder.builder(Serdes.Integer(), Serdes.Integer())
                    .withConfig(p)
                    .withConsumerConfig(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID))
                    .withSourceTopic(TOPIC)
                    .withExecutor(executor)
                    .withPartitionAssignmentEventConsumer(c -> e -> tpFuture.complete(e.partitions()))
                    .withRecordConsumer(r -> timedExecution(() -> {
                        logger.info("payload: {}-{}@{}", r.topic(), r.partition(), r.offset());
                        latch.countDown();
                    }, sleeptimeMsec));
            builder.build().start();
            KafkaSink<Integer,Integer> sink = builder
                    .withSinkTopic(TOPIC)
                    .build();
            sink.start();
            Flux.from(sink).log().subscribe(_r -> rmFuture.complete(_r));
            return timedExecution(() -> {
                Flux.range(1,100).map(i -> new ProducerRecord<Integer,Integer>(sink.topic(), i, i)).subscribe(sink);
                try {
                    assertEquals(Set.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)), tpFuture.get(3, TimeUnit.SECONDS)); // assert topic partition assignment
                    assertNotNull(rmFuture.get(3, TimeUnit.SECONDS)); // assert metadata returned on publish
                    latch.await(30, TimeUnit.SECONDS); // return the received message
                    assertEquals(0, latch.getCount());
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                } catch (TimeoutException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
        }).duration();
        logger.info("expected: {} actual: {}",(tolerance * (trials*sleeptimeMsec)/partitionCount), d.toMillis());
        assertTrue(d.toMillis() < (tolerance * (trials*sleeptimeMsec)/partitionCount)); // assert the received message is as expected
    }
}
