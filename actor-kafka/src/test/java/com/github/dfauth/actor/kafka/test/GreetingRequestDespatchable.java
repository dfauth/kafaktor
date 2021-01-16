package com.github.dfauth.actor.kafka.test;

import com.github.dfauth.actor.Addressable;
import com.github.dfauth.actor.AskPattern;

public interface GreetingRequestDespatchable extends AskPattern<GreetingRequest, GreetingResponse> {
    @Override
    default GreetingRequest toRequest(Addressable<GreetingResponse> r) {
        return null;
    }

    interface Builder<E extends GreetingRequestDespatchable.Builder<E>> {
    }
}
