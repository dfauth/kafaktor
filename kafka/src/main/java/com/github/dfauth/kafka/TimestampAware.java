package com.github.dfauth.kafka;

import java.time.Instant;

public interface TimestampAware<T> {

    T withTimestamp(Instant i);

    interface RecoveryStrategy<K,V> extends TimestampAware<com.github.dfauth.kafka.RecoveryStrategy<K,V>> {

    }
}
