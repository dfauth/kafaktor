package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.DeserializingFunction;
import com.github.dfauth.actor.kafka.EnvelopeHandler;
import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.github.dfauth.actor.kafka.avro.AddressDespatchable;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.TestModule;
import com.github.dfauth.kafka.RecoveryStrategies;
import com.github.dfauth.kafka.Stream;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.github.dfauth.kafaktor.bootstrap.Bootstrapper.name;
import static com.github.dfauth.kafka.KafkaTestUtil.embeddedKafkaWithTopic;
import static com.github.dfauth.trycatch.Try.tryWith;
import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static com.github.dfauth.utils.ConfigBuilder.builder;
import static com.github.dfauth.utils.ConfigBuilder.builderFrom;

public class BootstrapTest {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapTest.class);
    private static final String TOPIC = "topic";
    private ExecutorService executor = Executors.newCachedThreadPool();

    @Inject
    private EnvelopeHandler<SpecificRecordBase> envelopeHandler;

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

            Behavior.Factory<HelloWorldMain.SayHello> guardian = HelloWorldMain.create();

            Instant sod = Instant.from(LocalDate.now().atTime(LocalTime.of(6, 0)).atZone(ZoneId.systemDefault()));

            Bootstrapper.CachingBootstrapper<String, ActorMessage> bootstrapper = new Bootstrapper.CachingBootstrapper(
                    RecoveryStrategies.<String, ActorMessage>timeBased().withTimestamp(() -> sod),
                    envelopeTransformer().andThen(_p -> _p.mapPayload(HelloWorldMain.SayHello.class::cast))
                    );

            Stream<String, ActorMessage> stream0 = Stream.Builder.stringKeyBuilder(envelopeHandler.envelopeSerde())
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withGroupId(this.getClass().getCanonicalName())
                    .build();

            Publisher publisher =  new Publisher() {
                @Override
                public <R, T> CompletableFuture<RecordMetadata> publish(KafkaActorRef<R,?> recipient, R msg, Optional<KafkaActorRef<T,?>> optSender) {
                    return tryCatch(() -> {
                        ActorMessage payload = optSender
                                .map(s -> envelopeHandler.envelope(recipient.toAddress(), s.toAddress(), (SpecificRecordBase) msg))
                                .orElse(envelopeHandler.envelope(recipient.toAddress(), (SpecificRecordBase) msg));
                        CompletableFuture<RecordMetadata> f = stream0.send(payload.getRecipient().getTopic(), payload.getRecipient().getKey(), payload);
                        f.handle(tryWith()).thenApply(_try ->
                            _try.recover(e -> {
                                logger.error(e.getMessage(), e);
                            }).accept(m ->
                                logger.info("successfully published key: {}, value{} to topic: {}, partition: {}, offset: {}",payload.getRecipient(), payload, m.topic(),m.partition(),m.offset())
                            )
                        );
                        return f;
                    });
                }
            };

            Stream<String, ActorMessage> stream = Stream.Builder.stringKeyBuilder(envelopeHandler.envelopeSerde())
                    .withProperties(p)
                    .withTopic(TOPIC)
                    .withGroupId(this.getClass().getCanonicalName())
//                    .withKeyFilter(n -> n.equals(this.getClass().getCanonicalName()))
                    .withRecordProcessor(bootstrapper)
                    .withExecutor(executor)
                    .withPartitionAssignmentEventConsumer(c -> e -> {
                        e.onAssigment(_p -> {
                            logger.info("partitions assigned: "+_p);
                            Map<TopicPartition, Long> bo = c.beginningOffsets(_p);
                            Map<TopicPartition, Long> eo = c.endOffsets(_p);
                            _p.forEach(__p -> {
                                logger.info("partition: {} offsets beginning: {} current: {} end: {}",__p,bo.get(__p), c.position(__p),eo.get(__p));
                                bootstrapper.getRecoveryStrategy().invoke(c, __p);
                                bootstrapper.createActorSystem(__p.topic(), __p.partition(), guardian, publisher);
                            });
                        });
                        e.onRevocation(_p -> {
                            logger.info("partitions revoked: "+_p);
                            _p.forEach(__p -> Bootstrapper.CachingBootstrapper.lookup(name(__p)).ifPresent(b -> b.stop()));
                        });
                    })
                    .build();
            stream.start();
            Thread.sleep(5 * 1000);
            Greeting greeting = Greeting.newBuilder().setName("Fred").build();
            stream.send(TOPIC, "greeting", envelopeHandler.envelope(toAddress(TOPIC, "greeting"), toAddress(TOPIC,"sender"), greeting));
            Thread.sleep(5 * 1000);
        }));

    }

    private AddressDespatchable toAddress(String topic, String key) {
        return new AddressDespatchable() {
            @Override
            public String getTopic() {
                return topic;
            }

            @Override
            public String getKey() {
                return key;
            }
        };
    }

    private <T extends SpecificRecordBase> Function<ConsumerRecord<String, ActorMessage>, Envelope<T>> envelopeTransformer() {
        return r -> tryCatch(() -> {
            Class<T> classOfT = r.value().getPayloadType();
            DeserializingFunction<T> f = DeserializingFunction.fromDeserializer(envelopeHandler.serde(classOfT).deserializer());
            return r.value().asEnvelope(f);
        });

    }

}
