package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.*;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestActorCreationFunction<T> implements BehaviorFactory<T> {

    private static final Logger logger = LoggerFactory.getLogger(TestActorCreationFunction.class);

    private final Config config;

    public TestActorCreationFunction(Config config) {
        this.config = config;
    }

    @Override
    public Behavior<T> create(ActorContext<T> ctx) {
        return new Behavior<T>(){

            @Override
            public Behavior onMessage(Envelope<T> e) {
                logger.info("onMessage({})",e);
                return this;
            }
        };
    }
}
