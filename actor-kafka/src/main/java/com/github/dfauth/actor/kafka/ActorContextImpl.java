package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.ActorContext;
import com.github.dfauth.actor.ActorRef;

public class ActorContextImpl<T> implements ActorContext<T> {

    @Override
    public String id() {
        return null;
    }

    @Override
    public ActorRef<T> self() {
        return null;
    }
}
