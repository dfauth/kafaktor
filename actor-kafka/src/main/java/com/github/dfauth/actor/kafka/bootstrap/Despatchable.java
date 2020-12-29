package com.github.dfauth.actor.kafka.bootstrap;

import org.apache.avro.specific.SpecificRecord;

public interface Despatchable extends SpecificRecord {
    void despatch(DespatchableHandler h);
}
