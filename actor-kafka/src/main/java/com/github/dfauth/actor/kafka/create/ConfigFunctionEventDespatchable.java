package com.github.dfauth.actor.kafka.create;

public interface ConfigFunctionEventDespatchable extends Despatchable {

    @Override
    default void despatch(DespatchableHandler h) {
        h.handle(this);
    }
}
