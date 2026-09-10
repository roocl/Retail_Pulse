package com.retailpulse.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class RouteEvents extends ProcessFunction<IngestedRecord, IngestedRecord> {
    public static final OutputTag<String> DEAD_LETTERS = new OutputTag<String>("invalid-events") {};
    private transient ObjectMapper mapper;

    @Override
    public void processElement(IngestedRecord record, Context context, Collector<IngestedRecord> out) throws Exception {
        if (record.valid()) {
            out.collect(record);
        } else {
            if (mapper == null) mapper = new ObjectMapper();
            var deadLetter = mapper.createObjectNode();
            deadLetter.put("sourceTopic", record.topic);
            deadLetter.put("sourcePartition", record.partition);
            deadLetter.put("sourceOffset", record.offset);
            deadLetter.put("keyBase64", record.keyBase64);
            deadLetter.put("valueBase64", record.valueBase64);
            deadLetter.put("reason", record.error);
            context.output(DEAD_LETTERS, mapper.writeValueAsString(deadLetter));
        }
    }
}
