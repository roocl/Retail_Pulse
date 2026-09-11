package com.retailpulse.query;

import java.math.BigDecimal;
import java.time.Instant;

public record MinuteMetric(String dataset, Instant windowStart, Instant windowEnd, long resultVersion,
                           long paidOrders, long paidUsers, long paidQuantity, long refunds,
                           BigDecimal gmv, BigDecimal refundAmount) {}
