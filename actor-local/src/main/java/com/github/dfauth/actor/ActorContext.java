package com.github.dfauth.actor;

import org.slf4j.Logger;

public interface ActorContext<T> {

    String id();

    ActorRef<T> self();

    <R> ActorRef<R> spawn(Behavior<R> behavior, String name);

    Logger getLogger();
}
