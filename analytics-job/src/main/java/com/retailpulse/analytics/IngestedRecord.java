package com.retailpulse.analytics;

/** Flink POJO: transport metadata and validated JSON, without generic record/Kryo serialization. */
public class IngestedRecord {
    public String topic;
    public int partition;
    public long offset;
    public String keyBase64;
    public String valueBase64;
    public String json;
    public String eventId;
    public long eventTime;
    public String error;

    public IngestedRecord() {
    }

    public boolean valid() {
        return error == null;
    }
}
