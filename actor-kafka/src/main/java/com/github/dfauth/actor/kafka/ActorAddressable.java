package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class ActorAddressable<T> implements Addressable<T> {


    private final String name;

    public ActorAddressable(String name) {
        this.name = name;
    }

    @Override
    public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
//        ActorMessage.newBuilder().setMetadata(Map.of(ADDRESSABLE, name)).setPayload(t.)
        return null;
    }
}
