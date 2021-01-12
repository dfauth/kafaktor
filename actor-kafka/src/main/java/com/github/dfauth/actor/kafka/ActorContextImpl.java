package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Behavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActorContextImpl<T> implements ActorContext<T> {

    private static final Logger logger = LoggerFactory.getLogger(com.github.dfauth.actor.ActorContextImpl.class);

    @Override
    public String id() {
        return null;
    }

    @Override
    public ActorRef<T> self() {
        return null;
    }

    @Override
    public <R> ActorRef<R> spawn(Behavior.Factory<R> behaviorFactory, String name) {
        return null;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
