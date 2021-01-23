package com.github.dfauth.actor.kafka.avro;

import org.apache.avro.specific.SpecificRecord;

public interface Despatchable extends SpecificRecord {
    void despatch(DespatchableHandler h);
}
