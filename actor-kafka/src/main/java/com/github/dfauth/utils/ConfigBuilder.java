package com.github.dfauth.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.function.Function;


public class ConfigBuilder {

    private final Config config;
    private final Stack<String> ctx;

    private ConfigBuilder(Config config) {
        this(config,  new Stack());
    }

    private ConfigBuilder(Config config, Stack<String> ctx) {
        this.config = config;
        this.ctx = ctx;
    }

    public static ConfigBuilder builder() {
        return wrap(ConfigFactory.empty());
    }

    public static ConfigBuilder builderFrom(Config config) {
        return wrap(config);
    }

    private static ConfigBuilder wrap(Config config) {
        return new ConfigBuilder(config);
    }

    private static ConfigBuilder wrap(Config config, Stack<String> ctx) {
        return new ConfigBuilder(config, ctx);
    }

    public Config build() {
        return config;
    }

    public <T> T map(Function<Config, T> f) {
        return f.apply(config);
    }

    public ConfigBuilder node(String key) {
        ctx.push(key);
        return this;
    }

    public ConfigBuilder value(String key, Object value) {
        return wrap(config.withValue(unwind(key), ConfigValueFactory.fromAnyRef(value)), ctx);
    }

    public ConfigBuilder values(Map<String, Object> values) {
        final Config[] tmp = {config};
        values.entrySet().stream().forEach(e -> {
            tmp[0] = tmp[0].withValue(unwind(e.getKey()), ConfigValueFactory.fromAnyRef(e.getValue()));
        });
        return wrap(tmp[0], ctx);
    }

    private String unwind() {
        return unwind(Optional.empty());
    }

    private String unwind(String key) {
        return unwind(Optional.ofNullable(key));
    }

    private String unwind(Optional<String> optKey) {
        StringWriter buffer = new StringWriter();
        return optKey.map(k -> {
            ctx.iterator().forEachRemaining(s -> buffer.append(String.format("%s.",s)));
            buffer.append(k);
            return buffer.toString();
        }).orElseGet(() -> {
            Iterator<String> it = ctx.iterator();
            if(it.hasNext()) {
                buffer.append(it.next());
                it.forEachRemaining(s -> buffer.append(String.format(".%s",s)));
            }
            return buffer.toString();
        });
    }
}
