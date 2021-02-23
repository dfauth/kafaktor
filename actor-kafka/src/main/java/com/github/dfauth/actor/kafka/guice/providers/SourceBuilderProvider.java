package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.kafka.KafkaSource;
import com.typesafe.config.Config;
import org.apache.kafka.common.serialization.Serdes;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.HashMap;
import java.util.Map;

import static com.github.dfauth.utils.FunctionUtils.accumulateMap;
import static com.github.dfauth.utils.FunctionUtils.combineMap;


public class SourceBuilderProvider implements Provider<KafkaSource.Builder<String, byte[]>> {

    private KafkaSource.Builder<String, byte[]> sourceBuilder;

    @Inject
    public SourceBuilderProvider(Config config) {
        sourceBuilder = KafkaSource.Builder.builder(Serdes.ByteArray());
        Map<String, Object> props = config.getConfig("kafka").entrySet().stream().reduce(new HashMap<>(),
                accumulateMap(e -> e.getKey(), e -> e.getValue()),
                combineMap());
        sourceBuilder.withConfig(props);
    }

    @Override
    public KafkaSource.Builder get() {
        return sourceBuilder;
    }
}
