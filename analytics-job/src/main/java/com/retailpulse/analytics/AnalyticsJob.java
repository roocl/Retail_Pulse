package com.retailpulse.analytics;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public final class AnalyticsJob {
    private AnalyticsJob() {}

    public static void main(String[] args) throws Exception {
        JobSettings settings = JobSettings.from(args, System.getenv());
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        build(environment, settings);
        environment.execute("RetailPulse event governance");
    }

    static void build(StreamExecutionEnvironment environment, JobSettings settings) {
        environment.setParallelism(settings.parallelism());
        environment.getConfig().setAutoWatermarkInterval(200);
        environment.enableCheckpointing(settings.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointStorage(settings.checkpointDirectory());
        environment.getCheckpointConfig().setCheckpointTimeout(60000);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        environment.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Duration.ofSeconds(5)));

        KafkaSource<IngestedRecord> source = KafkaSource.<IngestedRecord>builder()
                .setBootstrapServers(settings.bootstrapServers()).setTopics(settings.inputTopic())
                .setGroupId(settings.groupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("enable.auto.commit", "false")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(new EventDeserializer()).build();

        var routed = environment.fromSource(source, EventWatermarks.strategy(
                        Duration.ofMillis(settings.outOfOrderMs()), Duration.ofMillis(settings.idleTimeoutMs())), "kafka-events")
                .uid("kafka-events-v1")
                .process(new RouteEvents()).name("validate-and-route").uid("validate-and-route-v1");

        routed.keyBy(record -> record.eventId).process(new DeduplicateEvents(settings.dedupTtlMs()))
                .name("deduplicate-event-id").uid("deduplicate-event-id-v1")
                .map(record -> record.json).returns(String.class)
                .name("valid-json").uid("valid-json-v1")
                .print("valid-event").name("valid-event-log").uid("valid-event-log-v1");

        KafkaSink<String> deadLetters = KafkaSink.<String>builder()
                .setBootstrapServers(settings.bootstrapServers())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(settings.deadLetterTopic()).setValueSerializationSchema(new SimpleStringSchema()).build())
                .build();
        routed.getSideOutput(RouteEvents.DEAD_LETTERS).sinkTo(deadLetters)
                .name("dead-letter-kafka").uid("dead-letter-kafka-v1");
    }
}
