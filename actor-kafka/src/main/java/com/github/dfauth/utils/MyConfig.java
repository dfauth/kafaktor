package com.github.dfauth.utils;

import com.typesafe.config.Config;

public class MyConfig {

    private static final String TOPIC = "topic";
    private final ConfigUtils config;

    public MyConfig(Config config) {
        this.config = ConfigUtils.wrap(config);
    }

    public String getTopic() {
        return config.getString("kafka.topic").orElse(TOPIC);
    }
}
