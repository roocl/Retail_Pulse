package com.retailpulse.analytics;

import org.apache.flink.api.java.utils.ParameterTool;
import java.io.Serializable;
import java.util.Map;

public record StorageSettings(String jdbcUrl, String user, String password, String dataset, long resultVersion,
                              int batchSize, long flushMs, int maxRetries) implements Serializable {
    public StorageSettings {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:clickhouse://")) {
            throw new IllegalArgumentException("a ClickHouse JDBC URL is required");
        }
        if (user == null || user.isBlank() || password == null || password.isBlank()) {
            throw new IllegalArgumentException("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD are required");
        }
        if (dataset == null || dataset.isBlank() || dataset.length() > 128 || resultVersion <= 0) {
            throw new IllegalArgumentException("dataset must contain 1..128 characters; result-version must be positive");
        }
        if (batchSize <= 0 || batchSize > 10000 || flushMs <= 0 || maxRetries < 0 || maxRetries > 10) {
            throw new IllegalArgumentException("batch-size must be in [1,10000], flush-ms positive, max-retries in [0,10]");
        }
    }

    public static StorageSettings from(String[] args, Map<String, String> environment) {
        var cli = ParameterTool.fromArgs(args);
        return new StorageSettings(
                cli.get("clickhouse-url", environment.getOrDefault("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://localhost:8123/retailpulse")),
                environment.getOrDefault("CLICKHOUSE_USER", "retailpulse"), environment.get("CLICKHOUSE_PASSWORD"),
                cli.get("dataset", environment.getOrDefault("RETAILPULSE_DATASET", "retailpulse")),
                Long.parseLong(cli.get("result-version", environment.getOrDefault("RETAILPULSE_RESULT_VERSION", "1"))),
                Integer.parseInt(cli.get("jdbc-batch-size", environment.getOrDefault("RETAILPULSE_JDBC_BATCH_SIZE", "500"))),
                Long.parseLong(cli.get("jdbc-flush-ms", environment.getOrDefault("RETAILPULSE_JDBC_FLUSH_MS", "1000"))),
                Integer.parseInt(cli.get("jdbc-max-retries", environment.getOrDefault("RETAILPULSE_JDBC_MAX_RETRIES", "3"))));
    }

    @Override
    public String toString() {
        return "StorageSettings[dataset=" + dataset + ", resultVersion=" + resultVersion + "]";
    }
}
