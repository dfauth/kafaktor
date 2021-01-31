package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.EnvelopeConsumer;
import com.github.dfauth.actor.Named;

public interface ActorContainer<T> extends Named.BehaviorFactory<T, EnvelopeConsumer<T>> {

    default EnvelopeConsumer<T> create(String name, Behavior.Factory<T> factory) {
        return withName(name).withBehaviorFactory(factory);
    }

}
