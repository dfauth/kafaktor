package com.github.dfauth.reactivestreams;

import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.function.Function;

public class BaseProcessor<I,O> extends BaseSubscriber<I> implements Flow.Processor<I,O> {

    private Optional<Flow.Subscriber<? super O>> optSubscriber = Optional.empty();

    private final Function<I,O> f;

    public static <T> Flow.Processor<T,T> identity() {
        return new BaseProcessor<>(Function.<T>identity()){};
    }

    protected BaseProcessor(Function<I, O> f) {
        this.f = f;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super O> subscriber) {
        Optional<Flow.Subscription> tmp;
        synchronized (this) {
            optSubscriber = Optional.ofNullable(subscriber);
            tmp = optSubscription;
        }
        tmp.ifPresent(s -> init(subscriber, s));
    }

    @Override
    public synchronized void onSubscribe(Flow.Subscription subscription) {
        Optional<Flow.Subscriber<? super O>> tmp;
        synchronized (this) {
            super.onSubscribe(subscription);
            tmp = optSubscriber;
        }
        tmp.ifPresent(s -> init(s, subscription));
    }

    protected void init(Flow.Subscriber<? super O> subscriber, Flow.Subscription subscription) {
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
