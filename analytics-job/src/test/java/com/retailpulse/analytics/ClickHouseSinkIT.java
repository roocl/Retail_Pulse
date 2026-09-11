package com.retailpulse.analytics;

import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class ClickHouseSinkIT {
    @Test
    void newerWholeRankingReplacesOldRanksIncludingEmptyResults() throws Exception {
        String dataset = UUID.randomUUID().toString();
        var totals = new CommerceTotals();
        totals.add(EventParserTest.parse(EventParserTest.VALID));
        var first = totals.finish(1768471200000L, 1768471260000L, "商品'\\一");
        var second = totals.finish(first.windowStart, first.windowEnd, "second");
        var settings = StorageSettings.from(new String[]{"--dataset", dataset}, System.getenv());
        int latest = 0;
        for (int version : new int[]{1, 2, 1, 2, 3, 2}) {
            latest = Math.max(latest, version);
            var configured = StorageSettings.from(new String[]{"--dataset", dataset, "--result-version", "" + version}, System.getenv());
            var values = version == 1 ? List.of(first, second) : version == 2 ? List.of(first) : List.<MinuteMetrics>of();
            var ranking = ProductRanking.of(first.windowStart, first.windowEnd, values, 2);
            var sink = ClickHouseSinks.rankings(configured);
            ((InputTypeConfigurable) sink).setInputType(Types.POJO(ProductRanking.class,
                    java.util.Map.of("products", Types.LIST(Types.POJO(MinuteMetrics.class)))), new ExecutionConfig());
            try (var test = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink))) {
                test.open();
                test.processElement(new StreamRecord<>(ranking));
                test.snapshot(1, 1);
            }
            try (var connection = DriverManager.getConnection(settings.jdbcUrl(), settings.user(), settings.password());
                 var query = connection.prepareStatement("SELECT product_id, gmv FROM product_top_n WHERE dataset = ? ORDER BY rank")) {
                query.setString(1, dataset);
                try (var result = query.executeQuery()) {
                    if (latest < 3) {
                        assertTrue(result.next());
                        assertEquals(latest == 1 ? "second" : first.productId, result.getString(1));
                        assertEquals(new BigDecimal("12.30"), result.getBigDecimal(2));
                        if (latest == 1) {
                            assertTrue(result.next());
                            assertEquals(first.productId, result.getString(1));
                        }
                    }
                    assertFalse(result.next());
                }
            }
        }
    }

    @Test
    void checkpointFlushesAnExactMinuteAndReplayDoesNotDoubleCountIt() throws Exception {
        var settings = StorageSettings.from(new String[]{"--dataset", UUID.randomUUID().toString()}, System.getenv());
        var totals = new CommerceTotals();
        totals.add(EventParserTest.parse(EventParserTest.VALID));
        var minute = totals.finish(1768471200000L, 1768471260000L, null);
        var sink = ClickHouseSinks.minutes(settings);
        ((InputTypeConfigurable) sink).setInputType(Types.POJO(MinuteMetrics.class), new ExecutionConfig());
        try (var test = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink))) {
            test.open();
            test.processElement(new StreamRecord<>(minute));
            test.processElement(new StreamRecord<>(minute));
            test.snapshot(1, 1);
            try (var connection = DriverManager.getConnection(settings.jdbcUrl(), settings.user(), settings.password());
                 var query = connection.prepareStatement("SELECT paid_orders, paid_users, gmv, toUnixTimestamp64Milli(window_start) FROM minute_metrics WHERE dataset = ?")) {
                query.setString(1, settings.dataset());
                try (var result = query.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(1, result.getLong(1));
                    assertEquals(1, result.getLong(2));
                    assertEquals(new BigDecimal("12.30"), result.getBigDecimal(3));
                    assertEquals(1768471200000L, result.getLong(4));
                    assertFalse(result.next());
                }
            }
        }
    }
}
