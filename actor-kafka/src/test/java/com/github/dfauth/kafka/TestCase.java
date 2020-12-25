package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.Try.tryWith;
import static org.junit.Assert.assertEquals;

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
            Stream stream = StreamBuilder.stringBuilder()
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withMessageConsumer(m ->
                            f.complete(m))
                    .withAssignmentConsumer(c -> _p -> logger.info("partitions: {}", _p))
                    .build();
            stream.start();

            CompletableFuture<RecordMetadata> _f = stream.send(TOPIC, MESSAGE);
            _f.thenAccept(r -> logger.info("metadata: {}", r));
            tryWith(() -> _f.get(3, TimeUnit.SECONDS)).recover(_t -> f.completeExceptionally(_t));
            return f;
        });

        String result = tryF.get(30, TimeUnit.SECONDS);
        logger.info("result: {}",result);
        assertEquals(MESSAGE, result);
    }
}
