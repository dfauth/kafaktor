package com.github.dfauth.reactivestreams;

import java.util.Optional;
import java.util.concurrent.Flow;

public class BasePublisher<O> implements Flow.Publisher<O> {

    protected Optional<Flow.Subscriber<? super O>> optSubscriber = Optional.empty();

    @Override
    public void subscribe(Flow.Subscriber<? super O> subscriber) {
        synchronized (this) {
            optSubscriber = Optional.ofNullable(subscriber);
        }
    }

    protected void init(Flow.Subscriber<? super O> subscriber, Flow.Subscription subscription) {
        subscriber.onSubscribe(subscription);
    }
}
