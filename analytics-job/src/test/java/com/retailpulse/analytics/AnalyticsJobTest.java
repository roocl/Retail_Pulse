package com.retailpulse.analytics;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsJobTest {
    @Test
    void buildsTheActualGraphWithoutGenericSerializationAndEnablesCheckpoints() throws Exception {
        try (var environment = StreamExecutionEnvironment.getExecutionEnvironment()) {
            environment.getConfig().disableGenericTypes();
            AnalyticsJob.build(environment, JobSettings.from(new String[0], Map.of()));
            var graph = environment.getStreamGraph();
            Set<String> ids = graph.getStreamNodes().stream()
                    .map(node -> node.getTransformationUID()).collect(Collectors.toSet());
            assertTrue(ids.containsAll(Set.of("kafka-events-v1", "validate-and-route-v1",
                    "deduplicate-event-id-v1", "valid-json-v1")));
            assertEquals(CheckpointingMode.EXACTLY_ONCE, graph.getCheckpointConfig().getCheckpointingMode());
            assertEquals(10000, graph.getCheckpointConfig().getCheckpointInterval());
            assertTrue(graph.getCheckpointConfig().isExternalizedCheckpointsEnabled());
        }
    }
}
