package com.github.dfauth.actor;

public interface BehaviorFactoryAware<T,R> {
    R withBehaviorFactory(Behavior.Factory<T> behavior);

    interface Consumer<T> extends BehaviorFactoryAware<T, Void> {
        @Override
        default Void withBehaviorFactory(Behavior.Factory<T> factory) {
            acceptBehaviorFactory(factory);
            return null;
        }

        void acceptBehaviorFactory(Behavior.Factory<T> factory);
    }
}
