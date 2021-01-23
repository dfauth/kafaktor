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

    static AddressDespatchable toAddressDespatchable(String topic, String key) {
        return new AddressDespatchable() {
            @Override
            public String getTopic() {
                return topic;
            }

            @Override
            public String getKey() {
                return key;
            }
        };
    }

    interface Builder<E extends AddressDespatchable.Builder<E>> {
    }
}
