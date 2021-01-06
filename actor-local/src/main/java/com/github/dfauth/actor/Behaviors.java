package com.github.dfauth.actor;

public class Behaviors {

    public static final <T> Final<T> stopped() {
        return e -> {
            throw new IllegalStateException("Final state should not process any messages");
        };
    }

    public static <T> Penultimate<T> penultimate(EnvelopeConsumer<T> c) {
        return e -> c.receive(e);
    }

    interface Penultimate<T> extends Behavior<T> {

        @Override
        default Behavior<T> onMessage(Envelope<T> e) {
            receive(e);
            return stopped();
        }

        void receive(Envelope<T> e);

    }

    interface Final<T> extends Behavior<T> {

        @Override
        default boolean isFinal() {
            return true;
        }
    }

}
