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
        environment.execute("RetailPulse commerce metrics");
    }

    static void build(StreamExecutionEnvironment environment, JobSettings settings) {
        environment.setParallelism(settings.parallelism());
        environment.getConfig().disableGenericTypes();
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
                .uid("kafka-events-v2")
                .process(new RouteEvents()).name("validate-and-route").uid("validate-and-route-v2");

        var unique = routed.keyBy(record -> record.eventId).process(new DeduplicateEvents())
                .name("deduplicate-event-id").uid("deduplicate-event-id-v2");
        unique.map(record -> record.json).returns(String.class)
                .name("valid-json").uid("valid-json-v1")
                .print("valid-event").name("valid-event-log").uid("valid-event-log-v1");
        var metrics = MetricsPipeline.attach(unique, settings.topN());
        unique.getSideOutput(DeduplicateEvents.LATE_EVENTS)
                .union(metrics.minutes().getSideOutput(DeduplicateEvents.LATE_EVENTS),
                        metrics.products().getSideOutput(DeduplicateEvents.LATE_EVENTS))
                .map(new JsonOutput<IngestedRecord>()).returns(String.class)
                .name("late-json").uid("late-json-v2")
                .print("late-event").name("late-event-log").uid("late-event-log-v2");
        metrics.ranking().getSideOutput(MetricsPipeline.LATE_PRODUCTS)
                .map(new JsonOutput<MinuteMetrics>()).returns(String.class)
                .name("late-product-json").uid("late-product-json-v2")
                .print("late-product").name("late-product-log").uid("late-product-log-v2");
        metrics.minutes().map(new JsonOutput<MinuteMetrics>()).returns(String.class)
                .name("minute-json").uid("minute-json-v2")
                .print("minute-metrics").name("minute-metrics-log").uid("minute-metrics-log-v2");
        metrics.ranking().map(new JsonOutput<ProductRanking>()).returns(String.class)
                .name("top-n-json").uid("top-n-json-v2")
                .print("product-top-n").name("product-top-n-log").uid("product-top-n-log-v2");

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
