package com.github.dfauth.actor;

public interface BehaviorFactoryAware<T,R> {
    R withBehaviorFactory(Behavior.Factory<T> behavior);
}
