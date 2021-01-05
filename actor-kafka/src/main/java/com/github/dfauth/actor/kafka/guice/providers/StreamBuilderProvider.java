package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.kafka.Stream;
import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.Serdes;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.HashMap;
import java.util.Map;


public class StreamBuilderProvider implements Provider<Stream.Builder<String, byte[]>> {

    private Stream.Builder<String, byte[]> streamBuilder;

    @Inject
    public StreamBuilderProvider(Config config) {
        streamBuilder = Stream.Builder.stringKeyBuilder(Serdes.ByteArray());
        Map<String, Object> props = config.getConfig("kafka").entrySet().stream().reduce(new HashMap<>(),
                (acc, e) -> {
                    acc.put(e.getKey(), e.getValue().unwrapped());
                    return acc;
                },
                (acc1, acc2) -> {
                    acc1.putAll(acc2);
                    return acc1;
                });
        streamBuilder.withProperties(props);
    }

    @Override
    public Stream.Builder get() {
        return streamBuilder;
    }
}
