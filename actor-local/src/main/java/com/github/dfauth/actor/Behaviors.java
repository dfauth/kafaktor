package com.github.dfauth.actor;

public class Behaviors {

    public static final <T> Behavior<T> FINAL() {
        return new Final<T>(){

            @Override
            public Behavior<T> onMessage(Envelope e) {
                return this;
            }
        };
    }

    interface Penultimate<T> extends Behavior<T> {
        @Override
        default Behavior<T> onMessage(Envelope<T> e) {
            receive(e);
            return FINAL();
        }

        void receive(Envelope<T> e);

        static <T> Penultimate<T> penultimate(EnvelopeConsumer<T> c) {
            return e -> c.receive(e);
        }
    }

    public interface Final<T> extends Behavior<T> {

        default boolean isFinal() {
            return true;
        }
    }

}
