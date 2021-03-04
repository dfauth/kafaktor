package com.github.dfauth.akka.typed.simple.kafka;

import akka.dispatch.DispatcherPrerequisites;
import akka.dispatch.ExecutorServiceConfigurator;
import akka.dispatch.ExecutorServiceFactory;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class KafkaExecutor extends ExecutorServiceConfigurator {

    private static final Logger logger = LoggerFactory.getLogger(KafkaExecutor.class);

    public KafkaExecutor(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        return () -> Executors.newSingleThreadExecutor(r -> new Thread(null, r, "kafka-executor"));
    }

}
