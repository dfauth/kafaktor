package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.Actor;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.MessageConsumer;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.typesafe.config.Config;

import java.util.function.Function;

import static com.github.dfauth.utils.ClassUtils.constructFromConfig;

public interface MessageConsumerEventDespatchable extends Despatchable, Function<Config, ActorRef> {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }

    default ActorRef apply(Config config) {
        return Actor.fromMessageConsumer((MessageConsumer<ActorMessage>)constructFromConfig(getImplementationClassName(), config));
    }

    String getImplementationClassName();

    interface Builder<E extends MessageConsumerEventDespatchable.Builder<E>> {

    }
}
