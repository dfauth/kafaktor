package com.github.dfauth.bootstrap;

import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.kafka.RecoveryStrategy;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Bootstrapper<T,K,V> {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrapper.class);

    private final String name;
    private final RecoveryStrategy<K, V> recoveryStrategy;
    private static final Map<String, Bootstrapper> instances = new HashMap<>();
    private Behavior<T> behaviour;

    public static final String name(TopicPartition topicPartition) {
        return String.format("%s-%d", topicPartition.topic(), topicPartition.partition());
    }

    public static final Optional<Bootstrapper> lookup(TopicPartition topicPartition) {
        return Optional.ofNullable(instances.get(name(topicPartition)));
    }

    public Bootstrapper(TopicPartition topicPartition, Behavior<T> behaviour, RecoveryStrategy<K, V> recoveryStrategy) {
        this.name = name(topicPartition);
        this.behaviour = behaviour;
        this.recoveryStrategy = recoveryStrategy;
    }

    public RecoveryStrategy<K, V> getRecoveryStrategy() {
        return recoveryStrategy;
    }

    public void start() {
        instances.put(name, this);
    }

    public boolean stop() {
        return instances.remove(name, this);
    }
}
