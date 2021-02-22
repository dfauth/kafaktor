package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.github.dfauth.actor.kafka.avro.AddressDespatchable;
import com.github.dfauth.partial.Tuple2;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.github.dfauth.actor.kafka.avro.AddressDespatchable.toAddress;

public interface EnvelopeHandler<T> {

    ActorMessage envelope(AddressDespatchable recipient, T payload);

    ActorMessage envelope(AddressDespatchable recipient, AddressDespatchable sender, T payload);

    ActorMessage envelope(AddressDespatchable key, Map<String,String> metadata, T payload);

    Tuple2<Map<String, String>, T> extract(ActorMessage actorMessage);

    T payload(ActorMessage actorMessage);

    Serializer<ActorMessage> envelopeSerializer();

    DeserializingFunction<ActorMessage> envelopeDeserializer();

    Serde<T> serde();

    <R extends SpecificRecordBase> Serde<R> serde(Class<R> classOfR);

    Serde<ActorMessage> envelopeSerde();

    static <T extends SpecificRecordBase> EnvelopeHandler<T> of(Serde<T> serde) {
        return new EnvelopeHandler<T>() {
            public ActorMessage envelope(AddressDespatchable recipient, T payload) {
                return EnvelopeHandler.<T>envelope(recipient, payload, serde.serializer());
            }

            public ActorMessage envelope(AddressDespatchable recipient, AddressDespatchable sender, T payload) {
                return EnvelopeHandler.<T>envelope(recipient, sender, Collections.emptyMap(), payload, serde.serializer());
            }

            public ActorMessage envelope(AddressDespatchable key, Map<String,String> metadata, T payload) {
                return EnvelopeHandler.<T>envelope(key, metadata, payload, serde.serializer());
            }

            public Tuple2<Map<String, String>, T> extract(ActorMessage actorMessage) {
                return EnvelopeHandler.extract(actorMessage, serde.deserializer());
            }

            public T payload(ActorMessage actorMessage) {
                return EnvelopeHandler.<T>payload(actorMessage, serde.deserializer());
            }

            public Serializer<ActorMessage> envelopeSerializer() {
                return (Serializer<ActorMessage>) serde.serializer();
            }

            public DeserializingFunction<ActorMessage> envelopeDeserializer() {
                return (DeserializingFunction<ActorMessage>) serde.deserializer();
            }

            public Serde<T> serde() {
                return serde;
            }

            public <R extends SpecificRecordBase> Serde<R> serde(Class<R> classOfR) {
                return (Serde<R>) serde;
            }

            public Serde<ActorMessage> envelopeSerde() {
                return (Serde<ActorMessage>) serde;
            }
        };
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(AddressDespatchable recipient, T record, Serializer<T> serializer) {
        return envelope(recipient, Collections.emptyMap(), record, serializer);
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(AddressDespatchable recipient, AddressDespatchable sender, Map<String, String> metadata, T record, Serializer<T> serializer) {
        return envelope(recipient, Optional.ofNullable(sender), metadata, record, serializer);
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(AddressDespatchable recipient, Map<String, String> metadata, T record, Serializer<T> serializer) {
        return envelope(recipient, Optional.empty(), metadata, record, serializer);
    }

    static <T extends SpecificRecordBase> ActorMessage envelope(AddressDespatchable recipient, Optional<AddressDespatchable> optSender, Map<String, String> metadata, T record, Serializer<T> serializer) {
        return ActorMessage.newBuilder()
                .setTimestamp(Instant.now().toEpochMilli())
                .setRecipient(toAddress(recipient))
                .apply(b -> optSender.map(s -> b.setSender(toAddress(s))).orElse(b))
                .setMetadata(metadata)
                .setPayloadSchema(record.getSchema().getFullName())
                .setPayload(ByteBuffer.wrap(serializer.serialize(record.getSchema().getFullName(), record)))
                .blah()
                .build();
    }

    static <T extends SpecificRecordBase> Tuple2<Map<String, String>, T> extract(ActorMessage actorMessage, Deserializer<T> deserializer) {
        return new Tuple2<>() {
            @Override
            public Map<String, String> _1() {
                return actorMessage.getMetadata();
            }

            @Override
            public T _2() {
                return payload(actorMessage, deserializer);
            }
        };
    }

    static <T extends SpecificRecordBase> T payload(ActorMessage actorMessage, Deserializer<T> deserializer) {
        return deserializer.deserialize(actorMessage.getPayloadSchema(), actorMessage.getPayload().array());
    }
}
