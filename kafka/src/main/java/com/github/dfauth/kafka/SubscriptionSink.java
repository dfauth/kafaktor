package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Flow;

public class SubscriptionSink<K,V> implements Flow.Processor<ProducerRecord<K, V>,RecordMetadata>, Flow.Subscription {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionSink.class);
    private final KafkaProducer<K, V> producer;
    private Optional<Flow.Subscriber<? super RecordMetadata>> optSubscriber = Optional.empty();

    public SubscriptionSink(KafkaProducer<K, V> p) {
        this.producer = p;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        subscription.request(Integer.MAX_VALUE);
    }

    @Override
    public void onNext(ProducerRecord<K, V> r) {
        producer.send(r, (m, e) -> {
            boolean b = m != null ?
                    optSubscriber.map(s -> {
                        s.onNext(m);
                        return true;
                    }).orElse(false) :
                    handleException(e);
        });
    }

    private boolean handleException(Throwable t) {
        close();
        return false;
    }

    @Override
    public void onError(Throwable t) {
        handleException(t);
    }

    @Override
    public void onComplete() {
        close();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super RecordMetadata> subscriber) {
        optSubscriber = Optional.of(subscriber);
        subscriber.onSubscribe(this);
    }

    @Override
    public void request(long n) {
        // ignore this; we are going to send a stream of metadata regardless
    }

    @Override
    public void cancel() {
        this.close();
    }

    public void close() {
        this.producer.flush();
        this.producer.close();
    }

    public void start() {
    }

    public void stop() {

    }
}
