package com.github.dfauth.utils;

import com.typesafe.config.Config;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.github.dfauth.trycatch.Try.trySilentlyWithCallable;
import static com.github.dfauth.trycatch.TryCatch.tryCatchSilentlyIgnore;


public class ConfigUtils {

    private final Config config;

    public static ConfigUtils wrap(Config config) {
        return new ConfigUtils(config);
    }

    public static <T> Optional<T> getOptionalOfT(Config config, Function<Config, T> f) {
        return trySilentlyWithCallable(() -> f.apply(config)).toOptional();
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

    public static Map<String,Object> getMap(Config config, String name) {
        return tryCatchSilentlyIgnore(() -> {
            return config.getConfig(name)
                    .entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().unwrapped()));
                }, Collections.emptyMap());
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

    public Map<String,Object> getMap(String name) {
        return getMap(config, name);
    }

    public Config nested() {
        return config;
    }

    public <T> T map(Function<Config, T> f) {
        return f.apply(config);
    }
}
