package com.retailpulse.query;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import java.util.UUID;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class MetricsHttpIT {
    @Autowired MockMvc http;
    @Autowired JdbcTemplate jdbc;

    @Test
    void exposesRangeAndMatchingRankingWithBoundedParameters() throws Exception {
        String dataset = UUID.randomUUID().toString();
        http.perform(get("/api/metrics/range").param("dataset", dataset)
                        .param("from", "2026-01-15T10:00:00Z").param("to", "2026-01-15T11:00:00Z"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items").isEmpty());
        http.perform(get("/api/metrics/top-products").param("dataset", dataset)
                        .param("windowStart", "2026-01-15T10:00:00Z").param("resultVersion", "1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.ready").value(false));
        for (String[] bad : new String[][]{{"from", "bad"}, {"from", "2026-01-15T11:00:00Z"},
                {"to", "2026-01-17T11:00:00Z"}, {"limit", "0"}, {"limit", "1441"}, {"dataset", " "}}) {
            var request = get("/api/metrics/range");
            request.param("from", bad[0].equals("from") ? bad[1] : "2026-01-15T10:00:00Z");
            request.param("to", bad[0].equals("to") ? bad[1] : "2026-01-15T11:00:00Z");
            if (!bad[0].equals("from") && !bad[0].equals("to")) request.param(bad[0], bad[1]);
            http.perform(request).andExpect(status().isBadRequest()).andExpect(content().contentTypeCompatibleWith("application/problem+json"));
        }
        http.perform(get("/api/metrics/top-products").param("windowStart", "2026-01-15T10:00:00Z")
                        .param("resultVersion", "0")).andExpect(status().isBadRequest());
        http.perform(get("/api/metrics/top-products").param("windowStart", "2026-01-15T10:00:01Z")
                        .param("resultVersion", "1")).andExpect(status().isBadRequest());
    }

    @Test
    void servesExactJsonThroughTheRealRepositoryAndReportsNoData() throws Exception {
        String dataset = UUID.randomUUID().toString();
        http.perform(get("/api/metrics/latest").param("dataset", dataset)).andExpect(status().isNoContent());
        jdbc.update("""
                INSERT INTO minute_metric_snapshots
                (dataset, result_version, window_start, window_end, paid_orders, gmv)
                VALUES (?, 9007199254740993, fromUnixTimestamp64Milli(1768471200000),
                fromUnixTimestamp64Milli(1768471260000), 9007199254740993, 9007199254740993.12)
                """, dataset);
        http.perform(get("/api/metrics/latest").param("dataset", dataset))
                .andExpect(status().isOk()).andExpect(jsonPath("$.gmv").value("9007199254740993.12"))
                .andExpect(jsonPath("$.paidOrders").value("9007199254740993"))
                .andExpect(jsonPath("$.resultVersion").value("9007199254740993"))
                .andExpect(jsonPath("$.windowStart").value("2026-01-15T10:00:00Z"));
        http.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
    }
}
