package com.github.dfauth.akka;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Bootstrapper<T> {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrapper.class);

    private final String name;
    private final RecoveryStrategy<String, T> recoveryStrategy;
    private static final Map<String, Bootstrapper> instances = new HashMap<>();
    private Behavior<T> behaviour;
    private ActorSystem<T> system;

    public static final String name(TopicPartition topicPartition) {
        return String.format("%s-%d", topicPartition.topic(), topicPartition.partition());
    }

    public static final Optional<Bootstrapper> lookup(TopicPartition topicPartition) {
        return Optional.ofNullable(instances.get(name(topicPartition)));
    }

    public Bootstrapper(TopicPartition topicPartition, Behavior<T> behaviour, RecoveryStrategy<String, T> recoveryStrategy) {
        this.name = name(topicPartition);
        this.behaviour = behaviour;
        this.recoveryStrategy = recoveryStrategy;
    }

    public RecoveryStrategy<String, T> getRecoveryStrategy() {
        return recoveryStrategy;
    }

    public void start() {
        instances.put(name, this);
        system = ActorSystem.create(this.behaviour, name);
    }

    public boolean stop() {
        return instances.remove(name, this);
    }
}
