package com.github.dfauth.reactivestreams;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;

public abstract class BaseSubscriber<T> implements Subscriber<T> {

    protected Optional<Subscription> optSubscription = Optional.empty();

    @Override
    public void onSubscribe(Subscription subscription) {
        optSubscription = Optional.ofNullable(subscription);
    }

    @Override
    public void onNext(T item) {

    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public void onComplete() {
        optSubscription = Optional.empty();
    }
}
