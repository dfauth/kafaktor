package com.github.dfauth.reactivestreams;

import java.util.Optional;
import java.util.concurrent.Flow;

public abstract class BaseSubscriber<T> implements Flow.Subscriber<T> {

    protected Optional<Flow.Subscription> optSubscription = Optional.empty();

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
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
