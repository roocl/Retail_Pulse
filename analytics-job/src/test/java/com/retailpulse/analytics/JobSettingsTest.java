package com.retailpulse.analytics;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobSettingsTest {
    @Test
    void cliOverridesEnvironmentAndDefaults() {
        var settings = JobSettings.from(new String[]{"--parallelism", "2", "--dedup-ttl-ms", "500"},
                Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092", "RETAILPULSE_ANALYTICS_PARALLELISM", "3"));
        assertEquals(2, settings.parallelism());
        assertEquals(500, settings.dedupTtlMs());
        assertEquals("kafka:29092", settings.bootstrapServers());
        assertEquals(30000, settings.outOfOrderMs());
    }

    @Test
    void rejectsUnsafeOrInvalidSettings() {
        for (String[] args : new String[][]{{"--parallelism", "0"}, {"--dedup-ttl-ms", "0"},
                {"--out-of-order-ms", "-1"}, {"--idle-timeout-ms", "0"}, {"--checkpoint-interval-ms", "1"},
                {"--checkpoint-directory", "relative/path"}, {"--dead-letter-topic", "commerce-events"}}) {
            assertThrows(IllegalArgumentException.class, () -> JobSettings.from(args, Map.of()));
        }
    }
}
