package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.github.dfauth.utils.ConfigUtils.wrap;


public class BootstrapActor<T extends SpecificRecordBase> implements Consumer<ConsumerRecord<String, byte[]>>, DespatchableHandler {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapActor.class);

    private static final String GROUP_ID = "groupId";
    private final Stream.Builder<String,byte[]> streamBuilder;
    private final EnvelopeHandlerImpl<T> envelopeHandler;
    private ConfigUtils config;
    private Stream stream;
    private String name;

    @Inject
    public BootstrapActor(Config config, Stream.Builder<String,byte[]> streamBuilder, EnvelopeHandlerImpl<T> envelopeHandlerImpl) {
        this.config = wrap(config);
        this.streamBuilder = streamBuilder;
        this.envelopeHandler = envelopeHandlerImpl;
        this.name = wrap(config).getString("actor.bootstrap").orElse("bootstrap");
    }

    public void start() {
        this.stream = streamBuilder.build(b ->
                        b.withGroupId(GROUP_ID)
                        .withTopic(config.map(c -> c.getString("kafka.topic")))
                        .withKeyFilter(
                                k -> name.equals(k)
                        )
                        .withRecordConsumer(this)
        );
        stream.start();
    }

    public void stop() {
        stream.stop();
    }

    @Override
    public void accept(ConsumerRecord<String, byte[]> record) {
        logger.info("received record: {}",record);
        ActorMessage actorMessage = envelopeHandler.envelopeDeserializer().deserialize(record.topic(), record.value());
        logger.info("received actor message: {}",actorMessage);
        BiFunction<String, byte[], ? extends Despatchable> xdeserializer = (t,p) -> (Despatchable) envelopeHandler.envelopeDeserializer().deserialize(t,p);
        Despatchable payload = actorMessage.mapPayload(xdeserializer);
        payload.despatch(this);
    }

    @Override
    public void handle(BehaviorFactoryEventDespatchable event) {
        logger.info("received payload message: {}",event);
        ActorRef actorRef = event.apply(config.nested());
        logger.info("created BehaviorFactory actor ref: {}",actorRef);
    }

    @Override
    public void handle(MessageConsumerEventDespatchable event) {
        logger.info("received payload message: {}",event);
        ActorRef actorRef = event.apply(config.nested());
        logger.info("created MessageConsumer actor ref: {}",actorRef);
    }

    @Override
    public void handle(EnvelopeConsumerEventDespatchable event) {
        logger.info("received payload message: {}",event);
        ActorRef actorRef = event.apply(config.nested());
        logger.info("created EnvelopeConsumer actor ref: {}",actorRef);
    }
}
