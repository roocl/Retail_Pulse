package com.retailpulse.producer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Instant;
import java.util.Objects;

@ConfigurationProperties("retailpulse.generator")
public record GenerationProperties(
        @DefaultValue("100") long count,
        @DefaultValue("100") long intervalMs,
        @DefaultValue("42") long seed,
        @DefaultValue("0.05") double duplicateRate,
        @DefaultValue("0.10") double outOfOrderRate,
        @DefaultValue("2026-01-15T10:00:00Z") Instant startTime) {

    public GenerationProperties {
        if (count < 0) {
            throw new IllegalArgumentException("count must be nonnegative (0 means continuous)");
        }
        if (intervalMs < 0) {
            throw new IllegalArgumentException("interval-ms must be nonnegative");
        }
        requireRate(duplicateRate, "duplicate-rate");
        requireRate(outOfOrderRate, "out-of-order-rate");
        Objects.requireNonNull(startTime, "start-time must not be null");
    }

    private static void requireRate(double rate, String name) {
        if (!Double.isFinite(rate) || rate < 0 || rate > 1) {
            throw new IllegalArgumentException(name + " must be finite and in [0, 1]");
        }
    }
}
