package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.kafka.avro.AddressDespatchable;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class AvroAddressable<T extends SpecificRecordBase> implements Addressable<T> {


    private final AddressDespatchable address;

    public AvroAddressable(AddressDespatchable address) {
        this.address = address;
    }

    @Override
    public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
//        ActorMessage.newBuilder().setMetadata(Map.of(ADDRESSABLE, name)).setPayload(t.)
        return null;
    }
}
