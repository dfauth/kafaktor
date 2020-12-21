package com.github.dfauth.actor;

public interface ActorContext<T> {

    String id();

    ActorRef<T> self();
}
