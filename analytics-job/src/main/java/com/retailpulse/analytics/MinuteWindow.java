package com.retailpulse.analytics;

import java.time.Duration;

public final class MinuteWindow {
    public static final Duration SIZE = Duration.ofMinutes(1);

    private MinuteWindow() {}

    public static long end(long timestamp) {
        return Math.addExact(timestamp - Math.floorMod(timestamp, SIZE.toMillis()), SIZE.toMillis());
    }
}
