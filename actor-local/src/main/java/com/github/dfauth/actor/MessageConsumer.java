package com.github.dfauth.actor;

@FunctionalInterface
public interface MessageConsumer<T> extends EnvelopeConsumer<T> {

    @Override
    default void receive(Envelope<T> e) {
        receiveMessage(e.payload());
    }

    void receiveMessage(T payload);
}
