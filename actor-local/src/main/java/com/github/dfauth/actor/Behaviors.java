package com.github.dfauth.actor;

import java.util.function.Function;

public class Behaviors {

    public static <T> Behavior<T> setup(Function<ActorContext<T>, Behavior<T>> f) {
        return new DeferredBehavior(f);
    }

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

    private static class DeferredBehavior<T> implements Behavior<T> {

        private final Function<ActorContext<T>, Behavior<T>> f;

        public DeferredBehavior(Function<ActorContext<T>, Behavior<T>> f) {
            this.f = f;
        }

        @Override
        public Behavior<T> onMessage(Envelope<T> e) {
            throw new UnsupportedOperationException();
        }
    }
}
