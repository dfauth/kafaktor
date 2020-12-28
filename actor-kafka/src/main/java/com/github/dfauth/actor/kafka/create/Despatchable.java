package com.github.dfauth.actor.kafka.create;

public interface Despatchable {
    void despatch(DespatchableHandler h);
}
