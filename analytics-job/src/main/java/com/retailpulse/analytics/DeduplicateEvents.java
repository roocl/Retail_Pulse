package com.retailpulse.analytics;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DeduplicateEvents extends KeyedProcessFunction<String, IngestedRecord, IngestedRecord> {
    private final long ttlMs;
    private transient ValueState<Boolean> seen;

    public DeduplicateEvents(long ttlMs) {
        if (ttlMs <= 0) throw new IllegalArgumentException("dedup TTL must be positive");
        this.ttlMs = ttlMs;
    }

    @Override
    public void open(OpenContext context) {
        var ttl = StateTtlConfig.newBuilder(Duration.ofMillis(ttlMs))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(100, true).build();
        var descriptor = new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(ttl);
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(IngestedRecord record, Context context, Collector<IngestedRecord> out) throws Exception {
        if (seen.value() == null) {
            seen.update(true);
            out.collect(record);
        }
    }
}
