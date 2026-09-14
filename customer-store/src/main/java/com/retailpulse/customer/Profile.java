package com.retailpulse.customer;

import java.math.BigDecimal;

public record Profile(String customerId, String country, int recencyDays, long orders,
                      BigDecimal purchaseAmount, BigDecimal cancellationAmount, String preferredProduct,
                      int rScore, int fScore, int mScore, String segment) {
    public Profile {
        if (customerId == null || customerId.isBlank() || orders < 1 || recencyDays < 0)
            throw new IllegalArgumentException("Invalid customer profile");
        purchaseAmount = purchaseAmount.setScale(6);
        cancellationAmount = cancellationAmount.setScale(6);
        if (purchaseAmount.signum() <= 0 || cancellationAmount.signum() > 0)
            throw new IllegalArgumentException("Invalid profile amounts");
    }
}
