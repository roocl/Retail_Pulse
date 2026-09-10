package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import com.retailpulse.common.EventType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.ArrayDeque;

final class CommerceEventGenerator {
    private final GenerationProperties properties;
    private final Random random;
    private long sequence;
    private CommerceEvent previous;
    private Instant maximumTime;
    private final ArrayDeque<CommerceEvent> unpaid = new ArrayDeque<>();
    private final ArrayDeque<CommerceEvent> refundable = new ArrayDeque<>();

    CommerceEventGenerator(GenerationProperties properties) {
        this.properties = properties;
        this.random = new Random(properties.seed());
    }

    GeneratedEvent next() {
        if (previous != null && random.nextDouble() < properties.duplicateRate()) {
            return new GeneratedEvent(previous, true, false);
        }
        boolean late = previous != null && random.nextDouble() < properties.outOfOrderRate();
        Instant time = late
                ? maximumTime.minusMillis(1 + random.nextInt(30_000))
                : properties.startTime().plusSeconds(sequence);
        EventType type = eventType(random.nextInt(100));
        CommerceEvent predecessor = switch (type) {
            case PAYMENT_COMPLETED -> unpaid.pollFirst();
            case REFUND_COMPLETED -> refundable.pollFirst();
            default -> null;
        };
        if ((type == EventType.PAYMENT_COMPLETED || type == EventType.REFUND_COMPLETED) && predecessor == null) {
            type = EventType.ORDER_CREATED;
        }
        int quantity = 1 + random.nextInt(3);
        BigDecimal amount = switch (type) {
            case PRODUCT_VIEW, PRODUCT_CLICK -> new BigDecimal("0.00");
            default -> BigDecimal.valueOf((100L + random.nextInt(49_901)) * quantity, 2);
        };
        String orderId = type == EventType.ORDER_CREATED ? "order-" + properties.seed() + "-" + sequence : null;
        String userId = "user-" + (1 + random.nextInt(100));
        String productId = "product-" + (1 + random.nextInt(50));
        if (predecessor != null) {
            orderId = predecessor.orderId();
            userId = predecessor.userId();
            productId = predecessor.productId();
            amount = predecessor.amount();
            quantity = predecessor.quantity();
            if (time.isBefore(predecessor.eventTime())) time = predecessor.eventTime();
        }
        late = maximumTime != null && time.isBefore(maximumTime);
        if (maximumTime == null || time.isAfter(maximumTime)) maximumTime = time;
        previous = new CommerceEvent(CommerceEvent.CURRENT_SCHEMA_VERSION,
                "evt-" + properties.seed() + "-" + sequence++, orderId,
                userId, productId, type, amount, quantity, time);
        if (type == EventType.ORDER_CREATED) unpaid.addLast(previous);
        if (type == EventType.PAYMENT_COMPLETED) refundable.addLast(previous);
        if (unpaid.size() > 1000) unpaid.removeFirst();
        if (refundable.size() > 1000) refundable.removeFirst();
        return new GeneratedEvent(previous, false, late);
    }

    private static EventType eventType(int draw) {
        if (draw < 40) return EventType.PRODUCT_VIEW;
        if (draw < 65) return EventType.PRODUCT_CLICK;
        if (draw < 80) return EventType.ADD_TO_CART;
        if (draw < 90) return EventType.ORDER_CREATED;
        if (draw < 98) return EventType.PAYMENT_COMPLETED;
        return EventType.REFUND_COMPLETED;
    }

    record GeneratedEvent(CommerceEvent event, boolean duplicate, boolean outOfOrder) {
    }
}
