package com.github.dfauth.actor.kafka.bootstrap;

import com.github.dfauth.kafka.Stream;
import com.github.dfauth.kafka.StreamBuilder;
import com.github.dfauth.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.github.dfauth.utils.ConfigUtils.wrap;


public class BootstrapActor implements Consumer<ConsumerRecord<String, byte[]>> {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapActor.class);

    private static final String GROUP_ID = "groupId";
    private ConfigUtils config;
    private Stream stream;
    private String name;

    public BootstrapActor(Config config) {
        this.config = wrap(config);
        this.name = this.config.getString("actor.bootstrap").orElse("bootstrap");
    }

    public void start() {
        Map<String, Object> p = config.map(c -> c.getConfig("kafka").entrySet().stream().reduce(new HashMap(), (acc, e) -> {
            acc.put(e.getKey(), e.getValue().unwrapped());
            return acc;
        }, (acc1, acc2) -> {
            acc1.putAll(acc2);
            return acc1;
        }));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        this.stream = StreamBuilder.<String, byte[]>builder()
                .withProperties(p)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.serdeFrom(new ByteArraySerializer(), new ByteArrayDeserializer()))
                .withTopic(config.map(c -> c.getString("kafka.topic")))
                .withKeyFilter(k -> k.equals(name))
                .withRecordConsumer(this)
                .build();
        stream.start();
    }

    public void stop() {
        stream.stop();
    }

    @Override
    public void accept(ConsumerRecord<String, byte[]> record) {
        logger.info("received record: {}",record);
    }
}
