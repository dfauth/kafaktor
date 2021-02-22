package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ReactiveTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ReactiveTestCase.class);
    private static final String TOPIC = "topic";
    private static final String GROUP_ID = "groupId";
    private static final String MESSAGE = "Hello World!";

    @Test
    public void testIt() {

        CompletableFuture<Collection<TopicPartition>> tpFuture = new CompletableFuture<>();
        CompletableFuture<RecordMetadata> rmFuture = new CompletableFuture<>();
        CompletableFuture<String> messageFuture = new CompletableFuture<>();

        String result = embeddedKafkaWithTopic(TOPIC).runTest(p -> {
            KafkaSource.Builder<String, String> builder = KafkaSource.Builder.builder()
                    .withConfig(p)
                    .withConsumerConfig(Collections.singletonMap(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID))
                    .withSourceTopic(TOPIC)
                    .withPartitionAssignmentEventConsumer(c -> e -> tpFuture.complete(e.partitions()))
                    .withMessageConsumer(m ->
                            messageFuture.complete(m)
                    );
            builder.build().start();
            KafkaSink<String,String> sink = builder
                    .withSinkTopic(TOPIC)
                    .build();
            sink.start();
            Flux.from(sink).log().subscribe(_r -> rmFuture.complete(_r));
            Mono.just(MESSAGE).map(_m -> sink.toProducerRecord(_m)).subscribe(sink);
            try {
                assertEquals(Collections.singleton(new TopicPartition(TOPIC, 0)), tpFuture.get(3, TimeUnit.SECONDS)); // assert topic partition assignment
                assertNotNull(rmFuture.get(3, TimeUnit.SECONDS)); // assert metadata returned on publish
                return messageFuture.get(3, TimeUnit.SECONDS); // return the received message
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
        assertEquals(MESSAGE, result); // assert the received message is as expected
    }
}
