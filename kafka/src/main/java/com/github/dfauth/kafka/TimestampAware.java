package com.github.dfauth.kafka;

import java.time.Instant;
import java.util.function.Supplier;

public interface TimestampAware<T> {

    T withTimestamp(Supplier<Instant> i);

    interface RecoveryStrategy<K,V> extends TimestampAware<com.github.dfauth.kafka.RecoveryStrategy<K,V>> {

    }
}
