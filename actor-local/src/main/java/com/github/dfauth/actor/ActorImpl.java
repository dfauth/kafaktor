package com.github.dfauth.actor;

public interface ActorImpl<T> {

    void start();

    void stop();

    ActorRef<T> ref();
}
