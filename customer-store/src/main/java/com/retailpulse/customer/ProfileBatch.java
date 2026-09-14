package com.retailpulse.customer;

import java.time.LocalDate;
import java.util.Objects;

public record ProfileBatch(String id, String dataset, String sourceRelease, String ruleVersion,
                           LocalDate observation, int windowDays) {
    public ProfileBatch {
        for (String value : new String[]{id,dataset,sourceRelease,ruleVersion})
            if (value == null || value.isBlank() || value.length() > 64) throw new IllegalArgumentException("Invalid batch identity");
        Objects.requireNonNull(observation);
        if (windowDays < 1 || windowDays > 366) throw new IllegalArgumentException("Invalid observation window");
    }
}
