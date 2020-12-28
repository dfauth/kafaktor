package com.github.dfauth.actor.kafka.bootstrap;

public interface DespatchableHandler {
    void handle(BehaviorFactoryEventDespatchable record);
}
