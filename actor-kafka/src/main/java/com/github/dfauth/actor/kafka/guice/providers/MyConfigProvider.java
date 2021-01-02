package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.utils.MyConfig;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Provider;


public class MyConfigProvider implements Provider<MyConfig> {

    private final MyConfig config;

    @Inject
    public MyConfigProvider(Config config) {
        this.config = new MyConfig(config);
    }

    @Override
    public MyConfig get() {
        return config;
    }
}
