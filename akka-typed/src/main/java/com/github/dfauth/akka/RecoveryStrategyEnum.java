package com.github.dfauth.akka;

import com.github.dfauth.kafka.OffsetManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.function.Supplier;

import static com.github.dfauth.kafka.OffsetManager.Utils.*;
import static com.github.dfauth.trycatch.TryCatch.tryCatchIgnore;

public enum RecoveryStrategyEnum implements RecoveryStrategy<String, byte[]> {

    TIME_BASED(timeBased()),
    SEEK_TO_START(seekToStart()),
    USE_CURRENT(current()),
    SEEK_TO_END(seekToEnd());

    private OffsetManager<String, byte[]> delegate;

    RecoveryStrategyEnum(OffsetManager<String, byte[]> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void invoke(KafkaConsumer<String, byte[]> c, TopicPartition p, Supplier<Instant> supplier) {
        tryCatchIgnore(() -> delegate.withKafkaConsumer(c).withTopicPartition(p).accept(supplier));
    }

}
