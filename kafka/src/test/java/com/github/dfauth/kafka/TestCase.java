package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
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
    public void testIt() {

        CompletableFuture<String> f = new CompletableFuture<>();
        String result = embeddedKafkaWithTopic(TOPIC).runTest(p -> {
            KafkaSource.Builder<String, String> builder = KafkaSource.Builder.builder()
                    .withConfig(p)
                    .withConsumerConfig(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID))
                    .withSourceTopic(TOPIC)
                    .withPartitionAssignmentEventConsumer(c -> e ->
                            logger.info("partition assignment event: {}", e))
                    .withMessageConsumer(m ->
                            f.complete(m));
            KafkaSource source = builder.build();
            KafkaSink<String,String> sink = builder
                    .withSinkTopic(TOPIC)
                    .build();
            source.start();
            sink.start();

            Mono.just(MESSAGE).map(sink.messageAdapter()).subscribe(sink);
            Flux.from(sink).log();
            try {
                return f.get(100, TimeUnit.SECONDS);
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
        assertEquals(MESSAGE, result);
    }

}
