package com.retailpulse.analytics;

import org.apache.flink.api.java.utils.ParameterTool;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

record JobSettings(String bootstrapServers, String inputTopic, String deadLetterTopic, String groupId,
                   int parallelism, long outOfOrderMs, long idleTimeoutMs, long dedupTtlMs,
                   long checkpointIntervalMs, String checkpointDirectory) {
    JobSettings {
        for (String text : new String[]{bootstrapServers, inputTopic, deadLetterTopic, groupId, checkpointDirectory}) {
            if (text == null || text.isBlank()) throw new IllegalArgumentException("configuration must not be blank");
        }
        if (inputTopic.equals(deadLetterTopic)) throw new IllegalArgumentException("input and dead-letter topics must differ");
        if (parallelism < 1 || parallelism > 128) throw new IllegalArgumentException("parallelism must be in [1,128]");
        if (outOfOrderMs < 0 || outOfOrderMs > Duration.ofDays(7).toMillis()) {
            throw new IllegalArgumentException("out-of-order-ms must be between 0 and 7 days");
        }
        if (idleTimeoutMs <= 0 || dedupTtlMs <= 0 || checkpointIntervalMs < 1000) {
            throw new IllegalArgumentException("idle/TTL must be positive; checkpoint interval must be >=1000ms");
        }
        if (!URI.create(checkpointDirectory).isAbsolute()) {
            throw new IllegalArgumentException("checkpoint directory must be an absolute URI");
        }
    }

    static JobSettings from(String[] args, Map<String, String> environment) {
        ParameterTool cli = ParameterTool.fromArgs(args);
        return new JobSettings(
                cli.get("bootstrap-servers", environment.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
                cli.get("input-topic", environment.getOrDefault("RETAILPULSE_KAFKA_TOPIC", "commerce-events")),
                cli.get("dead-letter-topic", environment.getOrDefault("RETAILPULSE_DEAD_LETTER_TOPIC", "commerce-events-dead-letter")),
                cli.get("group-id", environment.getOrDefault("RETAILPULSE_ANALYTICS_GROUP", "retailpulse-stage3")),
                Integer.parseInt(value(cli, environment, "parallelism", "1")),
                Long.parseLong(value(cli, environment, "out-of-order-ms", "30000")),
                Long.parseLong(value(cli, environment, "idle-timeout-ms", "10000")),
                Long.parseLong(value(cli, environment, "dedup-ttl-ms", "3600000")),
                Long.parseLong(value(cli, environment, "checkpoint-interval-ms", "10000")),
                value(cli, environment, "checkpoint-directory", "file:///opt/flink/checkpoints"));
    }

    private static String value(ParameterTool cli, Map<String, String> environment, String key, String fallback) {
        return cli.get(key, environment.getOrDefault("RETAILPULSE_ANALYTICS_" + key.toUpperCase(java.util.Locale.ROOT).replace('-', '_'), fallback));
    }
}
