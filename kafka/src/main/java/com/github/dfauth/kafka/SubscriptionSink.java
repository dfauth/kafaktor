package com.github.dfauth.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SubscriptionSink<K,V> implements KafkaSink<K, V>, Subscription {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionSink.class);
    private final KafkaProducer<K, V> producer;
    private final String topic;
    private Optional<Subscriber<? super RecordMetadata>> optSubscriber = Optional.empty();

    public SubscriptionSink(String topic, KafkaProducer<K, V> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
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
    public void subscribe(Subscriber<? super RecordMetadata> subscriber) {
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
