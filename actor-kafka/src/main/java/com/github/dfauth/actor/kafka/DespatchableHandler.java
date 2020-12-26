package com.github.dfauth.actor.kafka;

public interface DespatchableHandler {
    void handle(TestRecordDespatchable record);
    void handle(CreateActorEventDespatchable event);
    void handle(TestErrorDespatchable testErrorThingy);
}
