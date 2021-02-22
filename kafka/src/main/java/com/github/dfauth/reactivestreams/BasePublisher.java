package com.github.dfauth.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;

public class BasePublisher<O> implements Publisher<O> {

    protected Optional<Subscriber<? super O>> optSubscriber = Optional.empty();

    @Override
    public void subscribe(Subscriber<? super O> subscriber) {
        synchronized (this) {
            optSubscriber = Optional.ofNullable(subscriber);
        }
    }

    protected void init(Subscriber<? super O> subscriber, Subscription subscription) {
        subscriber.onSubscribe(subscription);
    }
}
