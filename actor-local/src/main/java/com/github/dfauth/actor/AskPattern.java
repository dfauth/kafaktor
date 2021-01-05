package com.github.dfauth.actor;

import java.util.function.Function;

public interface AskPattern<I,O> extends Function<Addressable<O>, I> {

    @Override
    default I apply(Addressable<O> r) {
        return toRequest(r);
    }

    I toRequest(Addressable<O> r);
}
