package com.retailpulse.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GovernanceOperatorsTest {
    private KeyedOneInputStreamOperatorTestHarness<String, IngestedRecord, IngestedRecord> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new DeduplicateEvents()),
                record -> record.eventId, Types.STRING);
    }

    @Test
    void routesOnlyInvalidRecordsToAuditableDeadLetters() throws Exception {
        try (var test = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new RouteEvents()))) {
            test.open();
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID), 123));
            test.processElement(new StreamRecord<>(EventParserTest.parse("bad json")));
            test.processElement(new StreamRecord<>(EventParserTest.parse(null)));
            assertEquals(1, test.extractOutputStreamRecords().size());
            assertEquals(123, test.extractOutputStreamRecords().get(0).getTimestamp());
            var side = test.getSideOutput(RouteEvents.DEAD_LETTERS);
            assertEquals(2, side.size());
            var dead = new ObjectMapper().readTree(side.poll().getValue());
            assertEquals("input", dead.get("sourceTopic").asText());
            assertEquals(17, dead.get("sourceOffset").asLong());
            assertEquals("YmFkIGpzb24=", dead.get("valueBase64").asText());
            assertTrue(dead.hasNonNull("reason"));
            assertTrue(new ObjectMapper().readTree(side.poll().getValue()).get("valueBase64").isNull());
        }
    }

    @Test
    void keepsDuplicatesSuppressedUntilWindowCloseAndRoutesLateRecords() throws Exception {
        try (var test = harness()) {
            test.open();
            var record = EventParserTest.parse(EventParserTest.VALID);
            test.processElement(new StreamRecord<>(record, record.eventTime));
            test.setStateTtlProcessingTime(7_200_000);
            test.processElement(new StreamRecord<>(record, record.eventTime));
            assertEquals(1, test.extractOutputStreamRecords().size());
            test.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(record.eventTime + 59_999));
            test.processElement(new StreamRecord<>(record, record.eventTime));
            assertEquals(1, test.extractOutputStreamRecords().size());
            assertEquals(1, test.getSideOutput(DeduplicateEvents.LATE_EVENTS).size());
            assertEquals(0, test.numEventTimeTimers());
        }
    }

    @Test
    void restoresSeenIdsAndEventTimeCleanupFromCheckpoint() throws Exception {
        try (var original = harness(); var restored = harness()) {
            original.open();
            var record = EventParserTest.parse(EventParserTest.VALID);
            original.processElement(new StreamRecord<>(record, record.eventTime));
            restored.initializeState(original.snapshot(1, 100));
            restored.open();
            restored.processElement(new StreamRecord<>(record, record.eventTime));
            assertTrue(restored.extractOutputStreamRecords().isEmpty());
            restored.processWatermark(new org.apache.flink.streaming.api.watermark.Watermark(record.eventTime + 59_999));
            restored.processElement(new StreamRecord<>(record, record.eventTime));
            assertTrue(restored.extractOutputStreamRecords().isEmpty());
            assertEquals(1, restored.getSideOutput(DeduplicateEvents.LATE_EVENTS).size());
        }
    }
}
