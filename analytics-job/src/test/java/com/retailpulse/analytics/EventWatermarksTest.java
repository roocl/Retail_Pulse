package com.retailpulse.analytics;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.clock.RelativeClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class EventWatermarksTest {
    static class Clock implements RelativeClock {
        long millis = 1;
        public long relativeTimeMillis() { return millis; }
        public long relativeTimeNanos() { return millis * 1_000_000; }
    }

    static class Output implements WatermarkOutput {
        long watermark = Long.MIN_VALUE;
        boolean idle;
        public void emitWatermark(Watermark value) { watermark = value.getTimestamp(); idle = false; }
        public void markIdle() { idle = true; }
        public void markActive() { idle = false; }
    }

    @Test
    void idlePartitionStopsBlockingAndInvalidEventsDoNotKeepItActive() {
        var clock = new Clock();
        var context = new WatermarkGeneratorSupplier.Context() {
            public MetricGroup getMetricGroup() { return new UnregisteredMetricsGroup(); }
            public RelativeClock getInputActivityClock() { return clock; }
        };
        var strategy = EventWatermarks.strategy(Duration.ofSeconds(1), Duration.ofSeconds(1));
        var activeGenerator = strategy.createWatermarkGenerator(context);
        var idleGenerator = strategy.createWatermarkGenerator(context);
        var output = new Output();
        var multiplexer = new WatermarkOutputMultiplexer(output);
        multiplexer.registerNewOutput("active", ignored -> {});
        multiplexer.registerNewOutput("idle", ignored -> {});
        var active = multiplexer.getImmediateOutput("active");
        var idle = multiplexer.getImmediateOutput("idle");
        var valid = EventParserTest.parse(EventParserTest.VALID);
        activeGenerator.onEvent(valid, 10000, active);
        activeGenerator.onPeriodicEmit(active);
        idleGenerator.onPeriodicEmit(idle);
        assertEquals(Long.MIN_VALUE, output.watermark);

        clock.millis = 1002;
        activeGenerator.onEvent(valid, 30000, active);
        activeGenerator.onPeriodicEmit(active);
        idleGenerator.onEvent(EventParserTest.parse("bad"), Long.MAX_VALUE, idle);
        idleGenerator.onPeriodicEmit(idle);
        multiplexer.onPeriodicEmit();
        assertEquals(28999, output.watermark);

        idleGenerator.onEvent(valid, 50000, idle);
        idleGenerator.onPeriodicEmit(idle);
        activeGenerator.onEvent(valid, 60000, active);
        activeGenerator.onPeriodicEmit(active);
        multiplexer.onPeriodicEmit();
        assertEquals(48999, output.watermark);
        assertFalse(output.idle);
    }
}
