package com.retailpulse.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailpulse.common.CommerceEvent;
import com.retailpulse.common.EventType;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(Binding.class)
            .withInitializer(context -> {
                try {
                    new YamlPropertySourceLoader().load("application", new ClassPathResource("application.yml"))
                            .forEach(source -> context.getEnvironment().getPropertySources().addLast(source));
                } catch (Exception exception) {
                    throw new IllegalStateException(exception);
                }
            });

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(GenerationProperties.class)
    static class Binding {
    }

    @Test
    void effectiveKafkaConfigurationEnablesBoundedIdempotentDelivery() {
        runner.run(context -> {
            assertNull(context.getStartupFailure());
            var values = context.getBean(ProducerFactory.class).getConfigurationProperties();
            assertEquals("all", values.get(ProducerConfig.ACKS_CONFIG));
            assertEquals("true", values.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG).toString());
            assertEquals("5", values.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).toString());
            assertEquals(Integer.MAX_VALUE, values.get(ProducerConfig.RETRIES_CONFIG));
            assertEquals("30000", values.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG).toString());
            assertEquals("10000", values.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG).toString());
            assertEquals("10000", values.get(ProducerConfig.MAX_BLOCK_MS_CONFIG).toString());
            assertEquals(100, context.getBean(GenerationProperties.class).count());
        });
    }

    @Test
    void kafkaSerializerPreservesTheIsoEventTimeContract() {
        runner.run(context -> {
            var values = context.getBean(ProducerFactory.class).getConfigurationProperties();
            assertEquals(JsonSerializer.class, values.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
            var event = new CommerceEvent(CommerceEvent.CURRENT_SCHEMA_VERSION, "event-1", "order-1", "user-1", "product-1",
                    EventType.PAYMENT_COMPLETED, new BigDecimal("12.30"), 1,
                    Instant.parse("2026-01-15T10:00:00.123Z"));
            try (var serializer = new JsonSerializer<CommerceEvent>()) {
                serializer.configure(values, false);
                var wire = new ObjectMapper().readTree(serializer.serialize("commerce-events", event));
                assertTrue(wire.get("eventTime").isTextual());
                assertEquals("2026-01-15T10:00:00.123Z", wire.get("eventTime").textValue());
            }
        });
    }

    @Test
    void environmentAndCommandLineOverrideYaml() {
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("testEnvironment", Map.of(
                        "RETAILPULSE_GENERATOR_COUNT", "12",
                        "RETAILPULSE_GENERATOR_INTERVAL_MS", "7",
                        "RETAILPULSE_GENERATOR_SEED", "99",
                        "RETAILPULSE_GENERATOR_DUPLICATE_RATE", "0.4",
                        "RETAILPULSE_GENERATOR_OUT_OF_ORDER_RATE", "0.6",
                        "RETAILPULSE_GENERATOR_START_TIME", "2026-09-09T00:00:00Z"))))
                .run(context -> {
                    var config = context.getBean(GenerationProperties.class);
                    assertEquals(12, config.count());
                    assertEquals(7, config.intervalMs());
                    assertEquals(99, config.seed());
                    assertEquals(.4, config.duplicateRate());
                    assertEquals(.6, config.outOfOrderRate());
                    assertEquals("2026-09-09T00:00:00Z", config.startTime().toString());
                });
        runner.withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                new SimpleCommandLinePropertySource("--retailpulse.generator.count=9",
                        "--retailpulse.generator.interval-ms=0", "--retailpulse.generator.seed=5",
                        "--retailpulse.generator.duplicate-rate=0", "--retailpulse.generator.out-of-order-rate=1",
                        "--retailpulse.generator.start-time=2026-09-10T00:00:00Z")))
                .run(context -> {
                    var config = context.getBean(GenerationProperties.class);
                    assertEquals(9, config.count());
                    assertEquals(0, config.intervalMs());
                    assertEquals(5, config.seed());
                    assertEquals(0, config.duplicateRate());
                    assertEquals(1, config.outOfOrderRate());
                    assertEquals("2026-09-10T00:00:00Z", config.startTime().toString());
                });
    }
}
