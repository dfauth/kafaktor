package com.github.dfauth.reactivestreams;

import com.github.dfauth.function.Function2;

import java.util.concurrent.Flow;

public class OneTimePublisher<O> extends BasePublisher<O> implements Flow.Subscription {

    private O element;

    public static final <O> OneTimePublisher<O> of(O element) {
        return new OneTimePublisher<>(element);
    }

    public OneTimePublisher(O element) {
        this.element = element;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super O> subscriber) {
        super.subscribe(subscriber);
        init(subscriber, this);
    }

    @Override
    public void request(long n) {
        new Thread(null, () -> {
            optSubscriber.map(Function2.<Flow.Subscriber>peek(s ->
                    s.onNext(element)))
                    .orElseThrow(() -> new IllegalStateException("No subscriber"));
        }, "onetimePublisherThread").start();
    }

    @Override
    public void cancel() {

    }
}
