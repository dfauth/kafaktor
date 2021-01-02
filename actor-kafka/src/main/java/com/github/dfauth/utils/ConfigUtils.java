package com.github.dfauth.utils;

import com.typesafe.config.Config;

import java.util.Optional;
import java.util.function.Function;

import static com.github.dfauth.trycatch.Try.tryWithCallable;


public class ConfigUtils {

    private final Config config;

    public static ConfigUtils wrap(Config config) {
        return new ConfigUtils(config);
    }

    public static <T> Optional<T> getOptionalOfT(Config config, Function<Config, T> f) {
        return tryWithCallable(() -> f.apply(config)).toOptional();
    }

    public static Optional<String> getString(Config config, String name) {
        return getOptionalOfT(config, c -> c.getString(name));
    }

    public static Optional<Integer> getInt(Config config, String name) {
        return getOptionalOfT(config, c -> c.getInt(name));
    }

    public static Optional<Boolean> getBoolean(Config config, String name) {
        return getOptionalOfT(config, c -> c.getBoolean(name));
    }

    public ConfigUtils(Config config) {
        this.config = config;
    }

    public Optional<String> getString(String name) {
        return getString(config, name);
    }

    public Optional<Integer> getInt(String name) {
        return getInt(config, name);
    }

    public Optional<Boolean> getBoolean(String name) {
        return getBoolean(config, name);
    }

    public Config nested() {
        return config;
    }

    public <T> T map(Function<Config, T> f) {
        return f.apply(config);
    }
}
