package com.github.dfauth.akka.typed.simple.kafka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.github.dfauth.akka.typed.simple.MailboxConverter;
import com.github.dfauth.akka.typed.simple.config.KafkaConfig;
import com.github.dfauth.kafka.AssignmentListener;
import com.github.dfauth.kafka.KafkaSource;
import com.github.dfauth.kafka.RecordProcessor;
import com.github.dfauth.partial.Tuple2;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;


public class KafkaMailbox extends MailboxConverter implements MailboxType, RecordProcessor<String, ActorMessage> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMailbox.class);

    private final KafkaConfig kafkaConfig;
    private Map<String, ActorRef> mailboxes = new HashMap<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public KafkaMailbox(ActorSystem.Settings settings, Config config) {
        super(settings, config);
        this.kafkaConfig = new KafkaConfig(ConfigFactory.load());
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

    @Override
    public MessageQueue create(Optional<ActorRef> optOwner, Optional<ActorSystem> system) {
        optOwner.map(o ->
                mailboxes.computeIfAbsent(o.path().toStringWithoutAddress(), k -> o)
        ).orElseThrow(() -> new IllegalStateException("no owner information found"));
        return new KafkaMessageQueue(kafkaConfig.actorMessageQueueCapacity());
    }

    private Consumer<Collection<TopicPartition>> handlePartitionAssignment(KafkaConsumer<String, ActorMessage> c, AssignmentListener.PartitionAssignmentEvent e) {
        return topicPartitions -> {};
    }

    private Consumer<Collection<TopicPartition>> handlePartitionRevocation(KafkaConsumer<String, ActorMessage> c, AssignmentListener.PartitionAssignmentEvent e) {
        return topicPartitions -> {};
    }

    @Override
    public CompletableFuture<Long> apply(ConsumerRecord<String, ActorMessage> r) {
        CompletableFuture<Long> f = CompletableFuture.completedFuture(r.offset());
        Optional.ofNullable(mailboxes.get(r.key())).ifPresent(ref -> ref.tell(Tuple2.of(f,r.value()),null));
        return f;
    }
}
