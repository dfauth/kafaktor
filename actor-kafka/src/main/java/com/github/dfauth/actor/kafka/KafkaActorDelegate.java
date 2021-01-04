package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.*;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.ProdModule;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.trycatch.Try;
import com.github.dfauth.utils.MyConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaActorDelegate<T extends SpecificRecordBase> implements ActorDelegate<T> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaActorDelegate.class);

    @Inject private Stream.Builder<String, byte[]> streamBuilder;
    @Inject private EnvelopeHandlerImpl<? extends SpecificRecordBase> envelopeHandler;
    @Inject private Config config;

    public KafkaActorDelegate() {
        Injector injector = Guice.createInjector(MyModules.getOrElse(new CommonModule(), new ProdModule()));
        injector.injectMembers(this);
    }

    @Override
    public ActorRef<T> fromMessageConsumer(MessageConsumer<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromEnvelopeConsumer(EnvelopeConsumer<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromBehavior(Behavior<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromBehaviorFactory(Behavior.Factory<T> f) {
        ActorImpl<T> actor = null;
        MyConfig myConfig = new MyConfig(config);
        try {
            //TODO
            Consumer<Envelope<T>> envelopeConsumer = e -> f.withActorContext(new ActorContextImpl<>()).onMessage(e);
            Function<byte[], Try<ActorMessage>> f1 = envelopeHandler.envelopeDeserializer().tryWithTopic(myConfig.getTopic());
            Function<ActorMessage, Envelope<T>> f2_1 = am -> Envelope.of((T)envelopeHandler.payload(am));
            Function<Try<ActorMessage>, Try<Envelope<T>>> f2 = t -> t.map(f2_1);
            Function<byte[], Try<Envelope<T>>> f3 = f1.andThen(f2);
            Function<ConsumerRecord<String, byte[]>, Long> c1 = r -> f3.apply(r.value()).map(e -> {
                envelopeConsumer.accept(e);
                return r.offset()+1;
            }).toOptional().orElse(r.offset()+1);

            Stream<String, byte[]> stream = streamBuilder.build(c ->
                    c.withTopic(myConfig.getTopic())
                            .withRecordProcessor(c1)
                            .withGroupId("name")
                    .withKeyFilter(k -> k.equals("fred"))
            );
            stream.start();
            actor = com.github.dfauth.actor.ActorContextImpl.<T>of(f);
            return actor.ref();
        } catch(RuntimeException e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            Optional.ofNullable(actor).ifPresent(a -> a.start());
        }
    }
}
