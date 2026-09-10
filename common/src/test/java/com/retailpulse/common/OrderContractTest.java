package com.retailpulse.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderContractTest {
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final String payment = """
            {"schemaVersion":2,"eventId":"e1","orderId":"o1","userId":"u1","productId":"p1",
             "eventType":"PAYMENT_COMPLETED","amount":12.30,"quantity":2,"eventTime":"2026-01-15T10:00:00Z"}
            """;

    @Test
    void requiresOrderIdentityAndExactCentsForTransactions() throws Exception {
        var event = mapper.readValue(payment, CommerceEvent.class);
        assertEquals("o1", mapper.valueToTree(event).get("orderId").asText());
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class, () -> mapper.readValue(payment.replace("\"o1\"", "null"), CommerceEvent.class));
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class, () -> mapper.readValue(payment.replace("12.30", "12.301"), CommerceEvent.class));
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class, () -> mapper.readValue(payment.replace("\"schemaVersion\":2", "\"schemaVersion\":1"), CommerceEvent.class));
    }
}
