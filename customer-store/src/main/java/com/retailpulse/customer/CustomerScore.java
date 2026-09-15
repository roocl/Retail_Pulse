package com.retailpulse.customer;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record CustomerScore(String customerId,BigDecimal value) {
    public CustomerScore {
        if(customerId==null || customerId.isBlank() || customerId.length()>64 || value==null || value.signum()<0 || value.compareTo(BigDecimal.ONE)>0)
            throw new IllegalArgumentException("Invalid customer score");
        value=value.setScale(12,RoundingMode.HALF_UP);
    }
}
