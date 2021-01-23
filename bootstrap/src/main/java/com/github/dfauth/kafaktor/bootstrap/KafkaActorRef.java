package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.Behavior;
import com.github.dfauth.actor.Behaviors;
import com.github.dfauth.actor.kafka.avro.AddressDespatchable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.github.dfauth.actor.kafka.avro.AddressDespatchable.toAddressDespatchable;
import static com.github.dfauth.trycatch.TryCatch.tryCatchIgnore;

public class KafkaActorRef<T,R> implements ActorRef<T> {

    private ParentContext<R> ctx;
    private final String name;

    public KafkaActorRef(ParentContext<R> ctx, String name) {
        this.ctx = ctx;
        this.name = name;
    }

    @Override
    public <S> CompletableFuture<S> ask(T t) {
        CompletableFuture<S> f= new CompletableFuture();
        tryCatchIgnore(() -> {
                    Behavior.Factory<S> factory = ctx -> Behaviors.penultimate(e -> f.complete(e.payload()));
                    ActorRef<S> ref = ctx.spawn(factory, UUID.randomUUID().toString());
                    ctx.publish(this, t, (KafkaActorRef<R, ?>) ref); // FIXME
        },
                e -> f.completeExceptionally(e)
        );
        return f;
    }

    @Override
    public String id() {
        return name;
    }

    @Override
    public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
        return optAddressable
                .map(a -> ctx.publish(this, t, (KafkaActorRef<R, ?>) a))
                .orElse(ctx.publish(this, t))
                .thenApply(ignored -> t);
    }

    public AddressDespatchable toAddress() {
        return toAddressDespatchable(ctx.getTopic(), name);
    }
}
