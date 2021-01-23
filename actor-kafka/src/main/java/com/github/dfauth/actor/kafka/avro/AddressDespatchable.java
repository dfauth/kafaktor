package com.github.dfauth.actor.kafka.avro;

public interface AddressDespatchable {

    default String topic() {
        return getTopic();
    }

    default String key() {
        return getKey();
    }

    String getTopic();

    String getKey();

    static Address toAddress(AddressDespatchable recipient) {
        return Address.newBuilder().setTopic(recipient.topic()).setKey(recipient.key()).build();
    }

    interface Builder<E extends AddressDespatchable.Builder<E>> {
    }
}
