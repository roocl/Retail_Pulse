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
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new DeduplicateEvents(1000)),
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
    void deduplicatesPerIdAndDoesNotExtendTtlOnDuplicates() throws Exception {
        try (var test = harness()) {
            test.open();
            test.setStateTtlProcessingTime(0);
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID), 123));
            test.setStateTtlProcessingTime(900);
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID), 124));
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID.replace("e1", "e2"))));
            assertEquals(2, test.extractOutputStreamRecords().size());
            test.setStateTtlProcessingTime(999);
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID)));
            assertEquals(2, test.extractOutputStreamRecords().size());
            test.setStateTtlProcessingTime(1000);
            test.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID)));
            assertEquals(3, test.extractOutputStreamRecords().size());
            assertEquals(123, test.extractOutputStreamRecords().get(0).getTimestamp());
        }
    }

    @Test
    void snapshotRestoresSeenIdsUntilProcessingTimeExpiry() throws Exception {
        try (var original = harness(); var restored = harness()) {
            original.open();
            original.setStateTtlProcessingTime(100);
            original.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID)));
            var snapshot = original.snapshot(1, 100);
            restored.initializeState(snapshot);
            restored.open();
            restored.setStateTtlProcessingTime(500);
            restored.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID)));
            assertTrue(restored.extractOutputStreamRecords().isEmpty());
            restored.setStateTtlProcessingTime(1100);
            restored.processElement(new StreamRecord<>(EventParserTest.parse(EventParserTest.VALID)));
            assertEquals(1, restored.extractOutputStreamRecords().size());
        }
    }
}
