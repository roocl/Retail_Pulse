package com.retailpulse.analytics;

import java.math.BigDecimal;

public class MinuteMetrics {
    public long windowStart;
    public long windowEnd;
    public String productId;
    public long paidOrders;
    public long paidUsers;
    public long paidQuantity;
    public long refunds;
    public BigDecimal gmv;
    public BigDecimal refundAmount;

    public MinuteMetrics() {}
}
