package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Addressable;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.github.dfauth.actor.kafka.ActorMessageDespatchable.ADDRESSABLE;

public class AvroAddressable<T extends SpecificRecordBase> implements Addressable<T> {


    private final String name;

    public AvroAddressable(String name) {
        this.name = name;
    }

    @Override
    public CompletableFuture<T> tell(T t, Optional<Addressable<T>> optAddressable) {
//        ActorMessage.newBuilder().setMetadata(Map.of(ADDRESSABLE, name)).setPayload(t.)
        return null;
    }
}
