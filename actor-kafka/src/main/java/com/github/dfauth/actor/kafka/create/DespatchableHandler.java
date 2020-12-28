package com.github.dfauth.actor.kafka.create;

public interface DespatchableHandler {
    void handle(ConfigFunctionEventDespatchable record);
}
