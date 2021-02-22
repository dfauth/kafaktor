package com.github.dfauth.reactivestreams;

import com.github.dfauth.function.Function2;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class OneTimePublisher<O> extends BasePublisher<O> implements Subscription {

    private O element;

    public static final <O> OneTimePublisher<O> of(O element) {
        return new OneTimePublisher<>(element);
    }

    public OneTimePublisher(O element) {
        this.element = element;
    }

    @Override
    public void subscribe(Subscriber<? super O> subscriber) {
        super.subscribe(subscriber);
        init(subscriber, this);
    }

    @Override
    public void request(long n) {
        new Thread(null, () -> {
            optSubscriber.map(Function2.<Subscriber>peek(s ->
                    s.onNext(element)))
                    .orElseThrow(() -> new IllegalStateException("No subscriber"));
        }, "onetimePublisherThread").start();
    }

    @Override
    public void cancel() {

    }
}
