package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Named;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Function;

public interface ActorContainer<K,V,T> extends Named.BehaviorFactory<T, ActorContainer.ConsumerRecordProcessor<K,V>> {

    default ConsumerRecordProcessor<K, V> create(String name, Behavior.Factory<T> factory) {
        return withName(name).withBehaviorFactory(factory);
    }

    interface ConsumerRecordProcessor<K,V> extends Function<ConsumerRecord<K,V>, Long> {
        @Override
        default Long apply(ConsumerRecord<K, V> r) {
            return process(r).next();
        }

        Offset process(ConsumerRecord<K,V> r);
    }

    interface Offset {
        long next();
    }
}
