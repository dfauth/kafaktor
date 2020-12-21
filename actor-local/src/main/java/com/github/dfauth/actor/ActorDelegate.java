package com.github.dfauth.actor;

public interface ActorDelegate<T> {

    ActorRef<T> fromMessageConsumer(MessageConsumer<T> c);

    ActorRef<T> fromEnvelopeConsumer(EnvelopeConsumer<T> c);

    ActorRef<T> fromBehavior(Behavior<T> c);

    ActorRef<T> fromBehaviorFactory(BehaviorFactory<T> f);
}
