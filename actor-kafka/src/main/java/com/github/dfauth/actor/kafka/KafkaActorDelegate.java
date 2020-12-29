package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.*;

import java.util.Optional;

public class KafkaActorDelegate<T> implements ActorDelegate<T> {

    @Override
    public ActorRef<T> fromMessageConsumer(MessageConsumer<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromEnvelopeConsumer(EnvelopeConsumer<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromBehavior(Behavior<T> c) {
        return fromBehaviorFactory(ctx -> e -> c.onMessage(e));
    }

    @Override
    public ActorRef<T> fromBehaviorFactory(Behavior.Factory<T> f) {
        ActorImpl<T> actor = null;
        try {
            actor = ActorContextImpl.<T>of(f);
            return actor.ref();
        } finally {
            Optional.ofNullable(actor).ifPresent(a -> a.start());
        }
    }
}
