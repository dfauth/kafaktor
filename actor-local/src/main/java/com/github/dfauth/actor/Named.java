package com.github.dfauth.actor;

public interface Named<T> {

    T withName(String name);

    interface BehaviorFactory<T,R> extends Named<BehaviorFactoryAware<T, R>> { }

}
