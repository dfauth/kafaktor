package com.github.dfauth.kafka;

import com.github.dfauth.trycatch.Try;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestCase.class);
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";
    private static final String MESSAGE = "Hello World!";

    @Test
    public void testIt() throws InterruptedException, ExecutionException, TimeoutException {

        CompletableFuture<String> f = new CompletableFuture<>();
        CompletableFuture<String> tryF = embeddedKafkaWithTopic(TOPIC).runTestFuture(p -> {
            p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            Stream stream = Stream.Builder.builder()
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withMessageConsumer(m ->
                            f.complete(m))
                    .withPartitionAssignmentEventConsumer(c -> e -> e.onAssigment(_p -> {
                        Map<TopicPartition, Long> bo = c.beginningOffsets(_p);
                        Map<TopicPartition, Long> eo = c.endOffsets(_p);
                        _p.forEach(__p -> logger.info("partition: {} offsets beginning: {} current: {} end: {}",__p,bo.get(__p), c.position(__p),eo.get(__p)));
                    }))
                    .build();
            stream.start();

            CompletableFuture<RecordMetadata> _f = stream.send(TOPIC, MESSAGE);
            _f.thenAccept(r -> logger.info("metadata: {}", r));
            Try.tryWith(() -> _f.get(3, TimeUnit.SECONDS)).recover(_t -> {
                f.completeExceptionally(_t);
            });
            return f;
        });

        String result = tryF.get(10, TimeUnit.SECONDS);
        logger.info("result: {}",result);
        assertEquals(MESSAGE, result);
    }
}
