package com.retailpulse.analytics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailpulse.common.CommerceEvent;
import com.retailpulse.common.EventType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.DateTimeException;
import java.util.Base64;
import java.util.Set;

final class EventParser {
    private static final Set<String> FIELDS = Set.of("schemaVersion", "eventId", "userId", "productId",
            "eventType", "amount", "quantity", "eventTime");
    private final ObjectMapper mapper = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    IngestedRecord parse(String topic, int partition, long offset, byte[] key, byte[] value) {
        IngestedRecord result = new IngestedRecord();
        result.topic = topic;
        result.partition = partition;
        result.offset = offset;
        result.keyBase64 = encode(key);
        result.valueBase64 = encode(value);
        result.eventTime = Long.MIN_VALUE;
        try {
            if (value == null) throw new IllegalArgumentException("null Kafka value (tombstone)");
            result.json = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value)).toString();
            JsonNode node = mapper.readTree(result.json);
            if (node == null || !node.isObject() || node.size() != FIELDS.size()) {
                throw new IllegalArgumentException("expected object with exactly the eight event fields");
            }
            for (String field : FIELDS) {
                if (!node.hasNonNull(field)) throw new IllegalArgumentException("missing/null field: " + field);
            }
            if (!node.get("amount").isNumber()) throw new IllegalArgumentException("amount must be a JSON number");
            CommerceEvent event = new CommerceEvent(integer(node, "schemaVersion"), text(node, "eventId"),
                    text(node, "userId"), text(node, "productId"), EventType.valueOf(text(node, "eventType")),
                    node.get("amount").decimalValue(), integer(node, "quantity"), Instant.parse(text(node, "eventTime")));
            long timestamp = event.eventTime().toEpochMilli();
            // Explicit epoch range keeps watermark subtraction away from overflow/sentinel values.
            if (timestamp < 0 || timestamp > 253402300799999L) {
                throw new IllegalArgumentException("eventTime must be between 1970 and year 9999");
            }
            result.eventId = event.eventId();
            result.eventTime = timestamp;
        } catch (IOException | IllegalArgumentException | DateTimeException | ArithmeticException exception) {
            result.error = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        }
        return result;
    }

    private static String text(JsonNode node, String name) {
        if (!node.get(name).isTextual()) throw new IllegalArgumentException(name + " must be text");
        return node.get(name).textValue();
    }

    private static int integer(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new IllegalArgumentException(name + " must be a 32-bit JSON integer");
        }
        return value.intValue();
    }

    private static String encode(byte[] bytes) {
        return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
    }
}
