package com.github.dfauth.actor;

public abstract class AbstractBehavior<T> implements Behavior<T> {

    private final ActorContext<T> context;

    public AbstractBehavior(ActorContext<T> context) {
        this.context = context;
    }

    protected ActorContext<T> getContext() {
        return context;
    }
}
