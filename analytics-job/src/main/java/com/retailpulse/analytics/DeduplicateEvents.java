package com.retailpulse.analytics;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DeduplicateEvents extends KeyedProcessFunction<String, IngestedRecord, IngestedRecord> {
    public static final OutputTag<IngestedRecord> LATE_EVENTS =
            new OutputTag<>("late-events", TypeInformation.of(IngestedRecord.class));
    private transient ValueState<Boolean> seen;

    @Override
    public void open(OpenContext context) {
        seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen-event-id", Boolean.class));
    }

    @Override
    public void processElement(IngestedRecord record, Context context, Collector<IngestedRecord> out) throws Exception {
        long close = MinuteWindow.end(record.eventTime) - 1;
        if (context.timerService().currentWatermark() >= close) {
            context.output(LATE_EVENTS, record);
        } else if (seen.value() == null) {
            seen.update(true);
            context.timerService().registerEventTimeTimer(close);
            out.collect(record);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<IngestedRecord> out) throws Exception {
        seen.clear();
    }
}
