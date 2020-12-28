package com.github.dfauth.actor;

public class Behaviors {

    public static <T> Behavior<T> FINAL() {
        return new Final();
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

    static class Final<T> implements MessageConsumer<T> {

        @Override
        public void receiveMessage(T payload) {}

        public boolean isFinal() {
            return true;
        }
    }

}
