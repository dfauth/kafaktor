package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.Actor;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.kafka.ActorMessage;
import com.typesafe.config.Config;

import java.util.function.Function;

import static com.github.dfauth.utils.ClassUtils.constructFromConfig;

public interface BehaviorFactoryEventDespatchable<E extends BehaviorFactoryEventDespatchable<E>> extends Despatchable, Function<Config, ActorRef> {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }

    default ActorRef apply(Config config) {
        return Actor.fromBehaviorFactory((Behavior.Factory<ActorMessage>)constructFromConfig(getImplementationClassName(), config));
    }

    String getImplementationClassName();
}
