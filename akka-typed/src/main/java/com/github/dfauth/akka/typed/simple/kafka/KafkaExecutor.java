package com.github.dfauth.akka.typed.simple.kafka;

import akka.dispatch.DispatcherPrerequisites;
import akka.dispatch.ExecutorServiceConfigurator;
import akka.dispatch.ExecutorServiceFactory;
import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.github.dfauth.akka.typed.simple.config.KafkaConfig;
import com.github.dfauth.kafka.AssignmentListener;
import com.github.dfauth.kafka.KafkaSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;


public class KafkaExecutor extends ExecutorServiceConfigurator implements Function<ConsumerRecord<String, ActorMessage>, Long> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaExecutor.class);
    private final KafkaConfig kafkaConfig;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private String name;

    public KafkaExecutor(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
        this.kafkaConfig = new KafkaConfig(ConfigFactory.load());
        name = String.format("%s-executor",kafkaConfig.topics().stream().collect(Collectors.joining("-")));
        KafkaSource source = KafkaSource.Builder.builder(kafkaConfig.envelopeHandler().envelopeSerde())
                .withConfig(kafkaConfig.properties())
                .withSourceTopics(kafkaConfig.topics())
                .withGroupId(kafkaConfig.groupId())
                .withExecutor(executorService)
                .withPartitionAssignmentEventConsumer(c -> e -> e.onAssigment(handlePartitionAssignment(c,e)).onRevocation(handlePartitionRevocation(c,e)))
                .withRecordProcessor(this)
                .build();
        source.start();
    }

    private Consumer<Collection<TopicPartition>> handlePartitionAssignment(KafkaConsumer<String, ActorMessage> c, AssignmentListener.PartitionAssignmentEvent e) {
        return topicPartitions -> {};
    }

    private Consumer<Collection<TopicPartition>> handlePartitionRevocation(KafkaConsumer<String, ActorMessage> c, AssignmentListener.PartitionAssignmentEvent e) {
        return topicPartitions -> {};
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        return () -> Executors.newSingleThreadExecutor(r -> new Thread(null, r, name));
    }

    @Override
    public Long apply(ConsumerRecord<String, ActorMessage> r) {
        return r.offset();
    }

}
