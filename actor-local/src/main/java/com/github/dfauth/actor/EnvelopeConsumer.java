package com.github.dfauth.actor;

import java.util.function.Consumer;

@FunctionalInterface
public interface EnvelopeConsumer<T> extends Behavior<T>, Consumer<Envelope<T>> {

    @Override
    default void accept(Envelope<T> e) {
        receive(e);
    }

    @Override
    default Behavior<T> onMessage(Envelope<T> e) {
        accept(e);
        return this;
    }

    void receive(Envelope<T> e);

}
