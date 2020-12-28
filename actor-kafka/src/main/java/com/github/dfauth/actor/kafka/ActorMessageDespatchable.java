package com.github.dfauth.actor.kafka;

public interface ActorMessageDespatchable extends Despatchable {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }
}
