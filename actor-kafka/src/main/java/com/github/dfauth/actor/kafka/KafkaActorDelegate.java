package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.*;
import com.github.dfauth.actor.kafka.guice.CommonModule;
import com.github.dfauth.actor.kafka.guice.MyModules;
import com.github.dfauth.actor.kafka.guice.ProdModule;
import com.github.dfauth.kafka.Stream;
import com.github.dfauth.kafka.StreamBuilder;
import com.github.dfauth.trycatch.Try;
import com.github.dfauth.utils.MyConfig;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.dfauth.partial.VoidFunction.toFunction;

public class KafkaActorDelegate<T extends SpecificRecordBase> implements ActorDelegate<T> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaActorDelegate.class);

    private final StreamBuilder<String, byte[]> streamBuilder;
    private final EnvelopeHandlerImpl<T> envelopeHandler;
    private final MyConfig config;

//    Config config = ConfigFactory.load()
//            .withFallback(ConfigFactory.systemProperties())
//            .withFallback(ConfigFactory.systemEnvironment());

    public KafkaActorDelegate() {
        Injector injector = Guice.createInjector(MyModules.getOrElse(new CommonModule(), new ProdModule()));
        streamBuilder = injector.getInstance(StreamBuilder.class);
        envelopeHandler = injector.getInstance(EnvelopeHandlerImpl.class);
        config = injector.getInstance(MyConfig.class);
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
        try {
            //TODO
            Consumer<Envelope<T>> envelopeConsumer = e -> f.withActorContext(null).onMessage(e);
            Function<byte[], Try<ActorMessage>> f1 = envelopeHandler.envelopeDeserializer().tryWithTopic(config.getTopic());
            Function<Try<ActorMessage>, Try<Envelope<T>>> f2 = t -> t.map(am -> Envelope.of(envelopeHandler.payload(am)));
            Function<byte[], Try<Envelope<T>>> f3 = f1.andThen(f2);
            Consumer<byte[]> c1 = toFunction(envelopeConsumer).mapConcat(f3.andThen(t -> {
                if (t.isFailure()) {
                    Throwable e = t.toFailure().exception();
                    logger.error(e.getMessage(), e);
                }
                return t.toOptional();
            }));

            Stream<String, byte[]> stream = streamBuilder.copyOf(c ->
                    c.withTopic(config.getTopic())
                            .withMessageConsumer(c1)
                            .withGroupId("name")
                    .withKeyFilter(k -> k.equals("fred"))
            ).build();
            stream.start();
            actor = ActorContextImpl.<T>of(f);
            return actor.ref();
        } catch(RuntimeException e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            Optional.ofNullable(actor).ifPresent(a -> a.start());
        }
    }
}
