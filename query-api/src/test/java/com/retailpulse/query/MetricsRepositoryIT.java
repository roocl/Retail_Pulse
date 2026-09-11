package com.retailpulse.query;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class MetricsRepositoryIT {
    @Test
    void rankingDistinguishesPendingEmptyAndReadyAndNeverMixesVersions() {
        var jdbc = new JdbcTemplate(new DriverManagerDataSource("jdbc:clickhouse://localhost:8123/retailpulse",
                "retailpulse", System.getenv("CLICKHOUSE_PASSWORD")));
        var repository = new MetricsRepository(jdbc);
        String dataset = UUID.randomUUID().toString();
        var window = Instant.parse("2026-01-15T10:00:00Z");
        assertFalse(repository.ranking(dataset, window, 1, 10).ready());
        jdbc.update("""
                INSERT INTO product_ranking_snapshots (dataset, result_version, window_start, window_end, products)
                VALUES (?, 1, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), [('商品A', 30.25, 2, 1, 3), ('商品B', 20.00, 1, 1, 1)])
                """, dataset, window.toEpochMilli(), window.plusSeconds(60).toEpochMilli());
        var ready = repository.ranking(dataset, window, 1, 1);
        assertTrue(ready.ready());
        assertEquals(1, ready.products().size());
        assertEquals("商品A", ready.products().get(0).productId());
        assertEquals(new BigDecimal("30.25"), ready.products().get(0).gmv());
        assertFalse(repository.ranking(dataset, window, 2, 10).ready());
        jdbc.update("""
                INSERT INTO product_ranking_snapshots (dataset, result_version, window_start, window_end, products)
                VALUES (?, 2, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), [])
                """, dataset, window.toEpochMilli(), window.plusSeconds(60).toEpochMilli());
        var empty = repository.ranking(dataset, window, 2, 10);
        assertTrue(empty.ready());
        assertTrue(empty.products().isEmpty());
        assertFalse(repository.ranking(dataset, window, 1, 10).ready());
    }

    @Test
    void rangeIsHalfOpenOrderedAndReportsTruncation() {
        var jdbc = new JdbcTemplate(new DriverManagerDataSource("jdbc:clickhouse://localhost:8123/retailpulse",
                "retailpulse", System.getenv("CLICKHOUSE_PASSWORD")));
        String dataset = UUID.randomUUID().toString();
        for (long offset : new long[]{120000, 0, 60000}) {
            jdbc.update("""
                    INSERT INTO minute_metric_snapshots (dataset, result_version, window_start, window_end)
                    VALUES (?, 1, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?))
                    """, dataset, 1768471200000L + offset, 1768471260000L + offset);
        }
        var repository = new MetricsRepository(jdbc);
        var start = Instant.parse("2026-01-15T10:00:00Z");
        var result = repository.range(dataset, start, start.plusSeconds(120), 10);
        assertEquals(2, result.items().size());
        assertEquals(start, result.items().get(0).windowStart());
        assertEquals(start.plusSeconds(60), result.items().get(1).windowStart());
        assertFalse(result.truncated());
        var limited = repository.range(dataset, start, start.plusSeconds(120), 1);
        assertEquals(1, limited.items().size());
        assertTrue(limited.truncated());
        assertEquals(start.plusSeconds(120), repository.latest(dataset).orElseThrow().windowStart());
    }

    @Test
    void latestReadsTheNewestWindowAndConvergedVersionWithoutLosingPrecision() {
        var jdbc = new JdbcTemplate(new DriverManagerDataSource(
                System.getenv().getOrDefault("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://localhost:8123/retailpulse"),
                "retailpulse", System.getenv("CLICKHOUSE_PASSWORD")));
        String dataset = "query-'" + UUID.randomUUID();
        for (int version : new int[]{2, 1, 2}) {
            jdbc.update("""
                    INSERT INTO minute_metric_snapshots
                    (dataset, result_version, window_start, window_end, paid_orders, paid_users, paid_quantity, refunds, gmv, refund_amount)
                    VALUES (?, ?, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), 4, 3, 5, 1, ?, 2.30)
                    """, dataset, version, 1768471200000L, 1768471260000L,
                    version == 2 ? new BigDecimal("9007199254740993.12") : BigDecimal.ONE);
        }
        var repository = new MetricsRepository(jdbc);
        var result = repository.latest(dataset).orElseThrow();
        assertEquals(2, result.resultVersion());
        assertEquals(Instant.parse("2026-01-15T10:00:00Z"), result.windowStart());
        assertEquals(new BigDecimal("9007199254740993.12"), result.gmv());
        assertEquals(3, result.paidUsers());
        assertTrue(repository.latest("missing-" + dataset).isEmpty());
    }
}
