package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Named;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

public interface ActorContainer<K,V,T> extends Named.BehaviorFactory<T, Consumer<ConsumerRecord<K,V>>> {

    default Consumer<ConsumerRecord<K,V>> create(String name, Behavior.Factory<T> factory) {
        return withName(name).withBehaviorFactory(factory);
    }
}
