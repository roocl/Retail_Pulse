package com.retailpulse.analytics;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class ClickHouseSinks {
    private ClickHouseSinks() {}

    public static SinkFunction<ProductRanking> rankings(StorageSettings settings) {
        return JdbcSink.sink("""
                INSERT INTO product_ranking_snapshots
                (dataset, result_version, window_start, window_end, products)
                VALUES (?, ?, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), ?)
                """, (statement, value) -> {
            statement.setString(1, settings.dataset());
            statement.setLong(2, settings.resultVersion());
            statement.setLong(3, value.windowStart);
            statement.setLong(4, value.windowEnd);
            Object[] products = value.products.stream().map(product -> new Object[]{product.productId,
                    product.gmv, product.paidOrders, product.paidUsers, product.paidQuantity}).toArray();
            statement.setArray(5, statement.getConnection().createArrayOf(
                    "Tuple(String, Decimal(38, 2), Int64, Int64, Int64)", products));
        }, execution(settings), connection(settings));
    }

    public static SinkFunction<MinuteMetrics> minutes(StorageSettings settings) {
        return JdbcSink.sink("""
                INSERT INTO minute_metric_snapshots
                (dataset, result_version, window_start, window_end, paid_orders, paid_users, paid_quantity, refunds, gmv, refund_amount)
                VALUES (?, ?, fromUnixTimestamp64Milli(?), fromUnixTimestamp64Milli(?), ?, ?, ?, ?, ?, ?)
                """, (statement, value) -> {
            statement.setString(1, settings.dataset());
            statement.setLong(2, settings.resultVersion());
            statement.setLong(3, value.windowStart);
            statement.setLong(4, value.windowEnd);
            statement.setLong(5, value.paidOrders);
            statement.setLong(6, value.paidUsers);
            statement.setLong(7, value.paidQuantity);
            statement.setLong(8, value.refunds);
            statement.setBigDecimal(9, value.gmv);
            statement.setBigDecimal(10, value.refundAmount);
        }, execution(settings), connection(settings));
    }

    private static JdbcExecutionOptions execution(StorageSettings settings) {
        return JdbcExecutionOptions.builder().withBatchSize(settings.batchSize())
                .withBatchIntervalMs(settings.flushMs()).withMaxRetries(settings.maxRetries()).build();
    }

    private static JdbcConnectionOptions connection(StorageSettings settings) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(settings.jdbcUrl()).withDriverName("com.clickhouse.jdbc.Driver")
                .withUsername(settings.user()).withPassword(settings.password()).build();
    }
}
