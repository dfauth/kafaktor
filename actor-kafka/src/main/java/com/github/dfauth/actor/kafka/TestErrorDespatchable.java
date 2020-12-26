package com.github.dfauth.actor.kafka;

public interface TestErrorDespatchable extends Despatchable {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }

}
