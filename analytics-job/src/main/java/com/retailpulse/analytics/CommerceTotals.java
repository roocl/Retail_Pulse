package com.retailpulse.analytics;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class CommerceTotals {
    public Map<String, Boolean> orders = new HashMap<>();
    public Map<String, Boolean> users = new HashMap<>();
    public long refunds;
    public long paidQuantity;
    public BigDecimal gmv = new BigDecimal("0.00");
    public BigDecimal refundAmount = new BigDecimal("0.00");

    public CommerceTotals() {}

    public void add(IngestedRecord event) {
        switch (event.eventType) {
            case PAYMENT_COMPLETED -> {
                if (orders.putIfAbsent(event.orderId, true) == null) {
                    users.put(event.userId, true);
                    paidQuantity = Math.addExact(paidQuantity, event.quantity);
                    gmv = gmv.add(event.amount);
                }
            }
            case REFUND_COMPLETED -> {
                refunds = Math.incrementExact(refunds);
                refundAmount = refundAmount.add(event.amount);
            }
            default -> {}
        }
    }

    public MinuteMetrics finish(long start, long end, String productId) {
        var result = new MinuteMetrics();
        result.windowStart = start;
        result.windowEnd = end;
        result.productId = productId;
        result.paidOrders = orders.size();
        result.paidUsers = users.size();
        result.paidQuantity = paidQuantity;
        result.refunds = refunds;
        result.gmv = gmv;
        result.refundAmount = refundAmount;
        return result;
    }
}
