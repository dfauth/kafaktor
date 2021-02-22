package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscription;

import java.util.function.Consumer;

public class ConsumingSubscriber<I> extends BaseSubscriber<I> {

    private Consumer<I> consumer;

    public static final <I> ConsumingSubscriber<I> of(Consumer<I> consumer) {
        return new ConsumingSubscriber<>(consumer);
    }

    public ConsumingSubscriber(Consumer<I> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        super.onSubscribe(subscription);
        subscription.request(Integer.MAX_VALUE);
    }

    @Override
    public void onNext(I i) {
        consumer.accept(i);
    }
}
