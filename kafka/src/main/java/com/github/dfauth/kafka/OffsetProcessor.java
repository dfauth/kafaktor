package com.github.dfauth.kafka;

import com.github.dfauth.reactivestreams.BaseProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;
import java.util.function.Function;

public class OffsetProcessor<K,V> extends BaseProcessor<Long, ConsumerRecord<K,V>> {

    private OffsetAndMetadata offset;

    protected OffsetProcessor() {
        super(ignored -> null);
    }

    @Override
    public void onNext(Long o) {
        offset = new OffsetAndMetadata(o);
    }

    public OffsetAndMetadata getOffset() {
        return offset;
    }
}
