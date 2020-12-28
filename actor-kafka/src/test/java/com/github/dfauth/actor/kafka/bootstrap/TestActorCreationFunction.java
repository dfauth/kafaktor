package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.EnvelopeConsumer;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class TestActorCreationFunction<T> implements Function<Config, EnvelopeConsumer<T>> {

    private static final Logger logger = LoggerFactory.getLogger(TestActorCreationFunction.class);

    @Override
    public EnvelopeConsumer<T> apply(Config config) {
        return e -> logger.info("received envelop: {}",e);
    }
}
