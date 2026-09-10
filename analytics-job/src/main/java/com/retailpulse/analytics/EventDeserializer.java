package com.retailpulse.analytics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EventDeserializer implements KafkaRecordDeserializationSchema<IngestedRecord> {
    private transient EventParser parser;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<IngestedRecord> out) {
        if (parser == null) parser = new EventParser();
        out.collect(parser.parse(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
    }

    @Override
    public TypeInformation<IngestedRecord> getProducedType() {
        return TypeInformation.of(IngestedRecord.class);
    }
}
