package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.*;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestActorCreationFunction implements Behavior.Factory<SpecificRecord> {

    private static final Logger logger = LoggerFactory.getLogger(TestActorCreationFunction.class);

    private final Config config;

    public TestActorCreationFunction(Config config) {
        this.config = config;
    }

    @Override
    public Behavior<SpecificRecord> withActorContext(ActorContext<SpecificRecord> ctx) {
        return new Behavior<>() {

            @Override
            public Behavior onMessage(Envelope<SpecificRecord> e) {
                logger.info("onMessage({})", e);
                return this;
            }
        };
    }
}
