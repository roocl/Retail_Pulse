package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import com.retailpulse.common.EventType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

final class SampleEvents {
    private static final BigDecimal ZERO = new BigDecimal("0.00");

    private SampleEvents() {
    }

    static List<CommerceEvent> all() {
        return List.of(
                event("evt-001", "user-1001", "product-2001", EventType.PRODUCT_VIEW, ZERO, 1, "2026-01-15T10:00:00Z"),
                event("evt-002", "user-1001", "product-2002", EventType.PRODUCT_VIEW, ZERO, 1, "2026-01-15T10:00:01Z"),
                event("evt-003", "user-1002", "product-2001", EventType.PRODUCT_VIEW, ZERO, 1, "2026-01-15T10:00:02Z"),
                event("evt-004", "user-1001", "product-2001", EventType.ADD_TO_CART, new BigDecimal("129.90"), 1, "2026-01-15T10:00:03Z"),
                event("evt-005", "user-1001", "product-2001", EventType.ORDER_CREATED, new BigDecimal("129.90"), 1, "2026-01-15T10:00:04Z"),
                event("evt-006", "user-1002", "product-2001", EventType.ORDER_CREATED, new BigDecimal("259.80"), 2, "2026-01-15T10:00:05Z")
        );
    }

    private static CommerceEvent event(
            String eventId,
            String userId,
            String productId,
            EventType eventType,
            BigDecimal amount,
            int quantity,
            String eventTime) {
        return new CommerceEvent(
                CommerceEvent.CURRENT_SCHEMA_VERSION,
                eventId,
                userId,
                productId,
                eventType,
                amount,
                quantity,
                Instant.parse(eventTime)
        );
    }
}
