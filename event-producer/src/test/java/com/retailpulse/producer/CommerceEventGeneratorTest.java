package com.retailpulse.producer;

import com.retailpulse.common.CommerceEvent;
import com.retailpulse.common.EventType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.EnumMap;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

class CommerceEventGeneratorTest {
    static GenerationProperties config(double duplicateRate, double outOfOrderRate) {
        return new GenerationProperties(100, 0, 42, duplicateRate, outOfOrderRate,
                Instant.parse("2026-01-15T10:00:00Z"));
    }

    @Test
    void sameSeedAndConfigReproduceEveryField() {
        var first = new CommerceEventGenerator(config(.2, .3));
        var second = new CommerceEventGenerator(config(.2, .3));
        for (int i = 0; i < 10_000; i++) {
            assertEquals(first.next(), second.next());
        }
    }

    @Test
    void pacingAndCountDoNotChangeTheSequenceButSeedDoes() {
        var first = new CommerceEventGenerator(config(.2, .3));
        var paced = new CommerceEventGenerator(new GenerationProperties(0, 50, 42, .2, .3,
                config(.2, .3).startTime()));
        var otherSeed = new CommerceEventGenerator(new GenerationProperties(100, 0, 43, .2, .3,
                config(.2, .3).startTime()));
        assertNotEquals(new CommerceEventGenerator(config(.2, .3)).next(), otherSeed.next());
        for (int i = 0; i < 100; i++) assertEquals(first.next(), paced.next());
    }

    @Test
    void ratesMatchObservedRecordsAndEventDistribution() {
        var generator = new CommerceEventGenerator(config(.2, .3));
        var ids = new HashSet<String>();
        var types = new EnumMap<EventType, Integer>(EventType.class);
        CommerceEvent previous = null;
        Instant maximum = Instant.MIN;
        int duplicates = 0;
        int late = 0;
        int total = 100_000;
        for (int i = 0; i < total; i++) {
            var generated = generator.next();
            var event = generated.event();
            assertEquals(!ids.add(event.eventId()), generated.duplicate());
            if (generated.duplicate()) {
                duplicates++;
                assertEquals(previous, event);
            } else {
                assertEquals(event.eventTime().isBefore(maximum), generated.outOfOrder());
                if (generated.outOfOrder()) late++;
                types.merge(event.eventType(), 1, Integer::sum);
                if (event.eventTime().isAfter(maximum)) maximum = event.eventTime();
            }
            assertEquals(2, event.amount().scale());
            assertTrue(event.amount().signum() >= 0);
            previous = event;
        }
        assertEquals(.2, duplicates / (double) (total - 1), .01);
        assertEquals(.3, late / (double) (total - duplicates - 1), .01);
        double[] weights = {.40, .25, .15, .10, .08, .02};
        int index = 0;
        for (EventType type : EventType.values()) {
            assertEquals(weights[index++], types.get(type) / (double) (total - duplicates), .01);
        }
    }

    @Test
    void boundaryRatesAreExact() {
        var normal = new CommerceEventGenerator(config(0, 0));
        var duplicate = new CommerceEventGenerator(config(1, 1));
        var late = new CommerceEventGenerator(config(0, 1));
        var original = duplicate.next().event();
        var firstTime = late.next().event().eventTime();
        Instant previous = Instant.MIN;
        for (int i = 0; i < 1000; i++) {
            var event = normal.next();
            assertFalse(event.duplicate());
            assertFalse(event.outOfOrder());
            assertTrue(event.event().eventTime().isAfter(previous));
            previous = event.event().eventTime();
            assertEquals(original, duplicate.next().event());
            assertTrue(late.next().event().eventTime().isBefore(firstTime));
        }
    }

    @Test
    void rejectsInvalidOptionsBeforeGeneration() {
        for (double rate : new double[]{-.1, 1.1, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> config(rate, 0));
            assertThrows(IllegalArgumentException.class, () -> config(0, rate));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new GenerationProperties(-1, 0, 42, 0, 0, Instant.EPOCH));
        assertThrows(IllegalArgumentException.class,
                () -> new GenerationProperties(1, -1, 42, 0, 0, Instant.EPOCH));
        assertDoesNotThrow(() -> new GenerationProperties(0, 0, 42, 0, 0, Instant.EPOCH));
    }
}
