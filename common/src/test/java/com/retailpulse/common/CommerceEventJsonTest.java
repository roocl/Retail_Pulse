package com.retailpulse.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommerceEventJsonTest {
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @ParameterizedTest
    @EnumSource(EventType.class)
    void shouldRoundTripTheEventContract(EventType eventType) throws Exception {
        CommerceEvent source = new CommerceEvent(
                CommerceEvent.CURRENT_SCHEMA_VERSION,
                "evt-001",
                "user-1001",
                "product-2001",
                eventType,
                new BigDecimal("129.90"),
                1,
                Instant.parse("2026-01-15T10:00:00Z")
        );

        String json = objectMapper.writeValueAsString(source);
        CommerceEvent restored = objectMapper.readValue(json, CommerceEvent.class);

        assertEquals(source, restored);
        assertTrue(json.contains("\"eventTime\":\"2026-01-15T10:00:00Z\""));
    }
}
