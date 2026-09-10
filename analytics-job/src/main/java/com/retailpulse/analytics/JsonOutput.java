package com.retailpulse.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class JsonOutput<T> extends RichMapFunction<T, String> {
    private transient ObjectMapper mapper;

    @Override
    public void open(OpenContext context) { mapper = new ObjectMapper(); }

    @Override
    public String map(T value) throws Exception { return mapper.writeValueAsString(value); }
}
