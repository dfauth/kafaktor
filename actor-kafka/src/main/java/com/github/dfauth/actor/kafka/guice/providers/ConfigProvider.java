package com.github.dfauth.actor.kafka.guice.providers;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Provider;


public class ConfigProvider implements Provider<Config> {

    private Config config = ConfigFactory.load()
            .withFallback(ConfigFactory.systemProperties())
            .withFallback(ConfigFactory.systemEnvironment());

    @Override
    public Config get() {
        return config;
    }
}
