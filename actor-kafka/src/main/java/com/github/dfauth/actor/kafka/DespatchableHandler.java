package com.github.dfauth.actor.kafka;

public interface DespatchableHandler {
    void handle(ActorMessageDespatchable event);
}
