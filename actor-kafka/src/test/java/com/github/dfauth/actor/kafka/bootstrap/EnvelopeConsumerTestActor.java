package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Envelope;
import com.github.dfauth.actor.EnvelopeConsumer;
import com.github.dfauth.actor.kafka.test.GreetingRequest;
import com.github.dfauth.actor.kafka.test.GreetingResponse;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class EnvelopeConsumerTestActor implements EnvelopeConsumer<GreetingRequest> {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeConsumerTestActor.class);

    private final Config config;

    public EnvelopeConsumerTestActor(Config config) {
        this.config = config;
    }

    @Override
    public void receive(Envelope<GreetingRequest> e) {
        logger.info("onMessage({})", e.payload());
        GreetingRequest greeting = e.payload();
        Optional<Addressable<GreetingResponse>> optSender = e.sender();
//        optSender.ifPresent(s -> {
//            logger.info("greeting {} from {}", greeting, s);
//            GreetingRequest req = greeting.apply(s);
//            GreetingResponse res = GreetingResponse.newBuilder().setPayload("Hi " + greeting.getName() + "!").build();
//            s.tell(res);
//        });
        e.replyWith(p -> GreetingResponse.newBuilder().setPayload(String.format("Hi %s!",p.getName())).build());
    }

}
