package com.github.dfauth.actor.kafka;

public interface Despatchable {
    void despatch(DespatchableHandler h);
}
