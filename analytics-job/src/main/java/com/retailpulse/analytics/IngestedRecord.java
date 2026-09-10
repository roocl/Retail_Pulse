package com.retailpulse.analytics;

import com.retailpulse.common.EventType;
import java.math.BigDecimal;

public class IngestedRecord {
    public String topic;
    public int partition;
    public long offset;
    public String keyBase64;
    public String valueBase64;
    public String json;
    public String eventId;
    public String orderId;
    public String userId;
    public String productId;
    public EventType eventType;
    public BigDecimal amount;
    public int quantity;
    public long eventTime;
    public String error;

    public IngestedRecord() {
    }

    public boolean valid() {
        return error == null;
    }
}
