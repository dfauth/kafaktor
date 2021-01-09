package com.github.dfauth.akka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Bootstrapper {

    private static final Logger logger = LoggerFactory.getLogger(Bootstrapper.class);

    private final String name;
    private RecoveryStrategy recoveryStrategy = RecoveryStrategyEnum.TIME_BASED;
    private static final Map<String, Bootstrapper> instances = new HashMap<>();

    public static final String name(TopicPartition topicPartition) {
        return String.format("%s-%d", topicPartition.topic(), topicPartition.partition());
    }

    public static final Optional<Bootstrapper> lookup(TopicPartition topicPartition) {
        return Optional.ofNullable(instances.get(name(topicPartition)));
    }
    public Bootstrapper(TopicPartition topicPartition) {
        this.name = name(topicPartition);
    }

    public RecoveryStrategy getRecoveryStrategy() {
        return recoveryStrategy;
    }

    public void start() {
        instances.put(name, this);
    }

    public boolean stop() {
        return instances.remove(name, this);
    }
}
