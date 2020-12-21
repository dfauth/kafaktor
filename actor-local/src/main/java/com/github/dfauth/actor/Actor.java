package com.github.dfauth.actor;

import java.util.ServiceLoader;

public class Actor<T> {

    public static <T> ActorRef<T> fromMessageConsumer(MessageConsumer<T> c) {
        return Actor.<T>loadDelegate().fromMessageConsumer(c);
    }

    public static <T> ActorRef<T> fromEnvelopeConsumer(EnvelopeConsumer<T> c) {
        return Actor.<T>loadDelegate().fromEnvelopeConsumer(c);
    }

    public static <T> ActorRef<T> fromBehavior(Behavior<T> c) {
        return Actor.<T>loadDelegate().fromBehavior(c);
    }

    public static <T> ActorRef<T> fromBehaviorFactory(BehaviorFactory<T> f) {
        return Actor.<T>loadDelegate().fromBehaviorFactory(f);
    }

    @SuppressWarnings("unchecked")
    public static <T> ActorDelegate<T> loadDelegate() {
        return ServiceLoader.load(ActorDelegate.class).findFirst().orElse(new LocalActorDelegate());
    }

}
