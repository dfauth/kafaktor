package com.github.dfauth.actor.kafka.bootstrap;

public interface DespatchableHandler {
    void handle(BehaviorFactoryEventDespatchable record);
    void handle(MessageConsumerEventDespatchable record);
    void handle(EnvelopeConsumerEventDespatchable record);
}