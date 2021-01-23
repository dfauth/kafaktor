package com.github.dfauth.actor.kafka.avro;

public interface DespatchableHandler {
    void handle(BehaviorFactoryEventDespatchable record);
    void handle(MessageConsumerEventDespatchable record);
    void handle(EnvelopeConsumerEventDespatchable record);
}
