package com.github.dfauth.actor.kafka;

import com.github.dfauth.partial.Tuple2;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EnvelopeHandlerImpl<T extends SpecificRecordBase> implements EnveloperHandler {

    private Serde<T> serde;

    public EnvelopeHandlerImpl(Serde<T> serde) {
        this.serde = serde;
    }

    public ActorMessage envelope(String key, T payload) {
        return EnveloperHandler.<T>envelope(key, payload, serde.serializer());
    }

    public ActorMessage envelope(String key, Map<String,String> metadata, T payload) {
        return EnveloperHandler.<T>envelope(key, metadata, payload, serde.serializer());
    }

    public Tuple2<Map<String, String>, T> extract(ActorMessage actorMessage) {
        return EnveloperHandler.extract(actorMessage, serde.deserializer());
    }

    public T payload(ActorMessage actorMessage) {
        return EnveloperHandler.<T>payload(actorMessage, serde.deserializer());
    }

    public Serializer<ActorMessage> envelopeSerializer() {
        return (Serializer<ActorMessage>) serde.serializer();
    }

    public DeserializingFunction<ActorMessage> envelopeDeserializer() {
        return (DeserializingFunction<ActorMessage>) serde.deserializer();
    }
}
