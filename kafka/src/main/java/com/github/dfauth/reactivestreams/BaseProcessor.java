package com.github.dfauth.reactivestreams;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Optional;
import java.util.function.Function;

public class BaseProcessor<I,O> extends BaseSubscriber<I> implements Processor<I,O> {

    private Optional<Subscriber<? super O>> optSubscriber = Optional.empty();

    private final Function<I,O> f;

    public static <T> Processor<T,T> identity() {
        return new BaseProcessor<>(Function.<T>identity()){};
    }

    protected BaseProcessor(Function<I, O> f) {
        this.f = f;
    }

    @Override
    public void subscribe(Subscriber<? super O> subscriber) {
        Optional<Subscription> tmp;
        synchronized (this) {
            optSubscriber = Optional.ofNullable(subscriber);
            tmp = optSubscription;
        }
        tmp.ifPresent(s -> init(subscriber, s));
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        Optional<Subscriber<? super O>> tmp;
        synchronized (this) {
            super.onSubscribe(subscription);
            tmp = optSubscriber;
        }
        tmp.ifPresent(s -> init(s, subscription));
    }

    protected void init(Subscriber<? super O> subscriber, Subscription subscription) {
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(I i) {
        optSubscriber.ifPresent(s -> s.onNext(f.apply(i)));
    }

    @Override
    public void onError(Throwable t) {
        optSubscriber.ifPresent(s -> s.onError(t));
    }
    @Override
    public void onComplete() {
        optSubscriber.ifPresent(s -> s.onComplete());
    }
}
