package com.retailpulse.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.time.Instant;
import java.util.Optional;
import java.util.List;
import java.math.BigDecimal;

@Repository
public class MetricsRepository {
    private static final String MINUTES = """
            SELECT dataset, toUnixTimestamp64Milli(window_start) AS start_ms,
                   toUnixTimestamp64Milli(window_end) AS end_ms, result_version,
                   paid_orders, paid_users, paid_quantity, refunds, gmv, refund_amount
            FROM minute_metrics
            """;
    private static final RowMapper<MinuteMetric> MINUTE = (row, number) -> new MinuteMetric(
            row.getString("dataset"), Instant.ofEpochMilli(row.getLong("start_ms")),
            Instant.ofEpochMilli(row.getLong("end_ms")), row.getLong("result_version"),
            row.getLong("paid_orders"), row.getLong("paid_users"), row.getLong("paid_quantity"),
            row.getLong("refunds"), row.getBigDecimal("gmv"), row.getBigDecimal("refund_amount"));
    private final JdbcTemplate jdbc;

    public MetricsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<MinuteMetric> latest(String dataset) {
        return jdbc.query(MINUTES + " WHERE dataset = ? ORDER BY window_start DESC LIMIT 1", MINUTE, dataset).stream().findFirst();
    }

    public record MinuteRange(List<MinuteMetric> items, boolean truncated) {}

    public MinuteRange range(String dataset, Instant from, Instant to, int limit) {
        var rows = jdbc.query(MINUTES + """
                 WHERE dataset = ? AND window_start >= fromUnixTimestamp64Milli(?)
                 AND window_start < fromUnixTimestamp64Milli(?) ORDER BY window_start LIMIT ?
                """, MINUTE, dataset, from.toEpochMilli(), to.toEpochMilli(), limit + 1);
        return new MinuteRange(List.copyOf(rows.subList(0, Math.min(limit, rows.size()))), rows.size() > limit);
    }

    public record RankedProduct(long rank, String productId, BigDecimal gmv, long paidOrders, long paidUsers, long paidQuantity) {}
    public record Ranking(boolean ready, List<RankedProduct> products) {}

    public Ranking ranking(String dataset, Instant windowStart, long version, int limit) {
        var rows = jdbc.query("""
                SELECT rank, product.product_id AS product_id, product.gmv AS gmv,
                       product.paid_orders AS paid_orders, product.paid_users AS paid_users,
                       product.paid_quantity AS paid_quantity
                FROM product_rankings
                LEFT ARRAY JOIN arraySlice(products, 1, ?) AS product,
                     arrayEnumerate(arraySlice(products, 1, ?)) AS rank
                WHERE dataset = ? AND window_start = fromUnixTimestamp64Milli(?) AND result_version = ?
                ORDER BY rank
                """, (row, number) -> new RankedProduct(row.getLong("rank"), row.getString("product_id"),
                row.getBigDecimal("gmv"), row.getLong("paid_orders"), row.getLong("paid_users"), row.getLong("paid_quantity")),
                limit, limit, dataset, windowStart.toEpochMilli(), version);
        return new Ranking(!rows.isEmpty(), rows.stream().filter(product -> product.rank() > 0).toList());
    }
}
