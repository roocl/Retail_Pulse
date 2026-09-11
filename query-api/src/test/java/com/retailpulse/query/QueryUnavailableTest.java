package com.retailpulse.query;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {"retailpulse.clickhouse.url=jdbc:clickhouse://127.0.0.1:1/retailpulse", "retailpulse.clickhouse.password=test-password"})
@AutoConfigureMockMvc
class QueryUnavailableTest {
    @Autowired MockMvc http;

    @Test
    void dashboardAssetsRemainAvailableWhenTheDatabaseIsDown() throws Exception {
        http.perform(get("/index.html")).andExpect(status().isOk())
                .andExpect(content().string(containsString("实时交易看板")));
        http.perform(get("/dashboard.js")).andExpect(status().isOk());
        http.perform(get("/dashboard.css")).andExpect(status().isOk());
    }

    @Test
    void unavailableDatabaseReturnsServiceUnavailableWithoutConnectionDetails() throws Exception {
        http.perform(get("/api/metrics/latest")).andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.detail").value("指标存储暂时不可用，请稍后重试"))
                .andExpect(content().string(not(containsString("jdbc:"))));
        http.perform(get("/actuator/health")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DOWN"));
    }
}
