package com.github.dfauth.actor.kafka.bootstrap;

public interface Despatchable {
    void despatch(DespatchableHandler h);
}
