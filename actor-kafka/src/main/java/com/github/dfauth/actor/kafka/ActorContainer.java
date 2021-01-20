package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Named;

public interface ActorContainer<K,V,T> extends Named.BehaviorFactory<T, ConsumerRecordProcessor<K,V>> {

    default ConsumerRecordProcessor<K, V> create(String name, Behavior.Factory<T> factory) {
        return withName(name).withBehaviorFactory(factory);
    }

}
