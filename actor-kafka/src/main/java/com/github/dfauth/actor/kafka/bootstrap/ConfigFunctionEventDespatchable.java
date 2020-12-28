package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.Actor;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.EnvelopeConsumer;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public interface ConfigFunctionEventDespatchable extends Despatchable, Function<Config, ActorRef> {

    Logger logger = LoggerFactory.getLogger(ConfigFunctionEventDespatchable.class);

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }

    default ActorRef apply(Config config) {
        try {
            Function<Config, EnvelopeConsumer<ActorMessage>> envelopeConsumerFn = (Function<Config, EnvelopeConsumer<ActorMessage>>) Class.forName(getImplementation()).getDeclaredConstructor(new Class[]{}).newInstance(new Object[]{});
            return Actor.fromEnvelopeConsumer(envelopeConsumerFn.apply(config));
        } catch (InstantiationException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    String getImplementation();
}
