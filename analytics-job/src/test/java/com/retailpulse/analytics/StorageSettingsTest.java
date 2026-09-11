package com.retailpulse.analytics;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class StorageSettingsTest {
    @Test
    void keepsRecalculationIdentityStableAndRejectsInvalidDeliveryOptions() {
        var environment = Map.of("CLICKHOUSE_PASSWORD", "test-secret", "RETAILPULSE_RESULT_VERSION", "7");
        var settings = StorageSettings.from(new String[]{"--result-version", "8", "--dataset", "test"}, environment);
        assertEquals(8, settings.resultVersion());
        assertEquals("test", settings.dataset());
        assertFalse(settings.toString().contains("test-secret"));
        for (String[] args : new String[][]{{"--result-version", "0"}, {"--jdbc-batch-size", "0"},
                {"--jdbc-flush-ms", "0"}, {"--jdbc-max-retries", "-1"}, {"--dataset", " "}}) {
            assertThrows(IllegalArgumentException.class, () -> StorageSettings.from(args, environment));
        }
        assertThrows(IllegalArgumentException.class, () -> StorageSettings.from(new String[0], Map.of()));
    }
}
