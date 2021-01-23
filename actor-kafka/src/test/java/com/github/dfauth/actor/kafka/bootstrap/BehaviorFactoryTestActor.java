package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BehaviorFactoryTestActor implements Behavior.Factory<ActorMessage> {

    private static final Logger logger = LoggerFactory.getLogger(BehaviorFactoryTestActor.class);

    private final Config config;

    public BehaviorFactoryTestActor(Config config) {
        this.config = config;
    }

    @Override
    public Behavior<ActorMessage> withActorContext(ActorContext<ActorMessage> ctx) {
        return new Behavior<>() {

            @Override
            public Behavior onMessage(Envelope<ActorMessage> e) {
                logger.info("onMessage({})", e.payload());
                return this;
            }
        };
    }
}
