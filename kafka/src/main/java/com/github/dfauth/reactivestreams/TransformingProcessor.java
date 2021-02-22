package com.github.dfauth.reactivestreams;

import java.util.function.Function;

public class TransformingProcessor<I,O> extends BaseProcessor<I,O> {

    public static <T,R> TransformingProcessor<T,R> of(Function<T,R> f) {
        return new TransformingProcessor<>(f);
    }

    protected TransformingProcessor(Function<I, O> f) {
        super(f);
    }
}
