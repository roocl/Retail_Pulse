package com.retailpulse.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventParserTest {
    static final String VALID = """
            {"schemaVersion":1,"eventId":"e1","userId":"u1","productId":"p1",
             "eventType":"PAYMENT_COMPLETED","amount":12.30,"quantity":1,"eventTime":"2026-01-15T10:00:00Z"}
            """;

    static IngestedRecord parse(String json) {
        return new EventParser().parse("input", 2, 17, new byte[]{1, 2},
                json == null ? null : json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void acceptsEveryEventTypeAndPreservesMetadata() {
        for (var type : com.retailpulse.common.EventType.values()) {
            var record = parse(VALID.replace("PAYMENT_COMPLETED", type.name()));
            assertTrue(record.valid(), record.error);
            assertEquals("e1", record.eventId);
            assertEquals(1768471200000L, record.eventTime);
            assertEquals("input", record.topic);
            assertEquals(2, record.partition);
            assertEquals(17, record.offset);
            assertEquals("AQI=", record.keyBase64);
            assertEquals(record.json, new String(Base64.getDecoder().decode(record.valueBase64), StandardCharsets.UTF_8));
        }
        assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(IngestedRecord.class));
    }

    @Test
    void rejectsMissingNullAndUnknownFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        var fields = List.of("schemaVersion", "eventId", "userId", "productId", "eventType", "amount", "quantity", "eventTime");
        for (String field : fields) {
            ObjectNode node = (ObjectNode) mapper.readTree(VALID);
            node.remove(field);
            assertFalse(parse(node.toString()).valid(), field);
            node.putNull(field);
            assertFalse(parse(node.toString()).valid(), field);
        }
        assertFalse(parse(VALID.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"extra\":0")).valid());
    }

    @Test
    void rejectsMalformedJsonCoercionsAndInvalidDomainFields() {
        for (String invalid : List.of("", "[]", "null", "{broken", VALID + "{}",
                VALID.replace("\"schemaVersion\":1", "\"schemaVersion\":1,\"schemaVersion\":1"),
                VALID.replace("\"schemaVersion\":1", "\"schemaVersion\":2"),
                VALID.replace("\"quantity\":1", "\"quantity\":0"),
                VALID.replace("\"quantity\":1", "\"quantity\":1.5"),
                VALID.replace("\"quantity\":1", "\"quantity\":\"1\""),
                VALID.replace("\"quantity\":1", "\"quantity\":2147483648"),
                VALID.replace("12.30", "-1"), VALID.replace("12.30", "-1e-999"), VALID.replace("12.30", "\"12.30\""),
                VALID.replace("\"e1\"", "\" \""), VALID.replace("\"u1\"", "4"),
                VALID.replace("PAYMENT_COMPLETED", "UNKNOWN"),
                VALID.replace("2026-01-15T10:00:00Z", "yesterday"),
                VALID.replace("2026-01-15T10:00:00Z", "1969-01-01T00:00:00Z"))) {
            assertFalse(parse(invalid).valid(), invalid);
        }
    }

    @Test
    void capturesTombstoneAndInvalidUtf8WithoutThrowing() {
        assertFalse(parse(null).valid());
        byte[] invalid = {(byte) 0xc3, (byte) 0x28};
        var record = new EventParser().parse("input", 0, 0, null, invalid);
        assertFalse(record.valid());
        assertArrayEquals(invalid, Base64.getDecoder().decode(record.valueBase64));
        assertEquals(Long.MIN_VALUE, record.eventTime);
    }
}
