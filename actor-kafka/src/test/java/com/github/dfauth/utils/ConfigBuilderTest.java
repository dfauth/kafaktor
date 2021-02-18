package com.github.dfauth.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigBuilderTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigBuilderTest.class);

    @Test
    public void testIt() {

        Config config = ConfigFactory.parseString(CONFIG);

        assertEquals(config, ConfigBuilder.builder()
                .value("kafka.topic", "TOPIC")
                .value("kafka.schema.registry.url", "http://localhost:8080")
                .value("kafka.schema.registry.capacity", 1024)
                .value("kafka.schema.registry.autoRegisterSchema", true)
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .value("schema.registry.url", "http://localhost:8080")
                .value("schema.registry.capacity", 1024)
                .value("schema.registry.autoRegisterSchema", true)
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .node("schema")
                .value("registry.url", "http://localhost:8080")
                .value("registry.capacity", 1024)
                .value("registry.autoRegisterSchema", true)
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .node("schema")
                .node("registry")
                .value("url", "http://localhost:8080")
                .value("capacity", 1024)
                .value("autoRegisterSchema", true)
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .node("schema")
                .node("registry")
                .values(Map.of("url", "http://localhost:8080"))
                .value("capacity", 1024)
                .value("autoRegisterSchema", true)
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .node("schema")
                .node("registry")
                .value("capacity", 1024)
                .value("autoRegisterSchema", true)
                .values(Map.of("url", "http://localhost:8080"))
                .build());

        assertConfig(config, ConfigBuilder.builder()
                .node("kafka")
                .value("topic", "TOPIC")
                .node("schema")
                .value("registry.capacity", 1024)
                .value("registry.autoRegisterSchema", true)
                .values(Map.of("registry.url", "http://localhost:8080"))
                .build());

    }

    private void assertConfig(Config expected, Config actual) {
        assertEquals(expected.entrySet(), actual.entrySet());
    }

    private static final String CONFIG = "{\n" +
            "  kafka {\n" +
            "    topic: \"TOPIC\"\n" +
            "    schema.registry.url: \"http://localhost:8080\"\n" +
            "    schema.registry.capacity: 1024\n" +
            "    schema.registry.autoRegisterSchema: true\n" +
            "  }\n" +
            "}";

}
