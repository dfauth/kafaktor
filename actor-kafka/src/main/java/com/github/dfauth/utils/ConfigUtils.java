package com.github.dfauth.utils;

import com.typesafe.config.Config;

import java.util.Optional;
import java.util.function.Function;

import static com.github.dfauth.trycatch.Try.tryWith;


public class ConfigUtils {

    private final Config config;

    public static ConfigUtils wrap(Config config) {
        return new ConfigUtils(config);
    }

    public ConfigUtils(Config config) {
        this.config = config;
    }

    public Optional<String> getString(String name) {
        return tryWith(() -> {
            return config.getString(name);
        }).toOptional();
    }

    public Config nested() {
        return config;
    }

    public <T> T map(Function<Config, T> f) {
        return f.apply(config);
    }
}
