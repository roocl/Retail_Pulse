package com.retailpulse.analytics;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

final class EventWatermarks {
    private EventWatermarks() {}

    static WatermarkStrategy<IngestedRecord> strategy(Duration outOfOrder, Duration idleTimeout) {
        WatermarkStrategy<IngestedRecord> base = WatermarkStrategy
                .<IngestedRecord>forBoundedOutOfOrderness(outOfOrder).withIdleness(idleTimeout);
        return ((WatermarkStrategy<IngestedRecord>) context -> {
            WatermarkGenerator<IngestedRecord> delegate = base.createWatermarkGenerator(context);
            return new WatermarkGenerator<>() {
                @Override
                public void onEvent(IngestedRecord record, long timestamp, WatermarkOutput output) {
                    // Bad input cannot advance event time or keep a partition active forever.
                    if (record.valid()) delegate.onEvent(record, timestamp, output);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    delegate.onPeriodicEmit(output);
                }
            };
        }).withTimestampAssigner((record, previousTimestamp) -> record.eventTime);
    }
}
