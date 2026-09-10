package com.retailpulse.common;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Objects;

public record CommerceEvent(
        int schemaVersion,
        String eventId,
        String orderId,
        String userId,
        String productId,
        EventType eventType,
        BigDecimal amount,
        int quantity,
        Instant eventTime
) {
    public static final int CURRENT_SCHEMA_VERSION = 2;

    public CommerceEvent {
        if (schemaVersion != CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported schemaVersion: " + schemaVersion);
        }
        requireText(eventId, "eventId");
        requireText(userId, "userId");
        requireText(productId, "productId");
        Objects.requireNonNull(eventType, "eventType must not be null");
        switch (eventType) {
            case ORDER_CREATED, PAYMENT_COMPLETED, REFUND_COMPLETED -> requireText(orderId, "orderId");
            default -> {
                if (orderId != null) throw new IllegalArgumentException("behavior events must not have an orderId");
            }
        }
        Objects.requireNonNull(amount, "amount must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        if (amount.signum() < 0) {
            throw new IllegalArgumentException("amount must not be negative");
        }
        amount = amount.setScale(2, RoundingMode.UNNECESSARY);
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive");
        }
    }

    private static void requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }
}
