package com.github.dfauth.actor.kafka;

public interface CreateActorEventDespatchable extends Despatchable {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }
}
