package com.retailpulse.query;

import com.retailpulse.customer.Profile;
import com.retailpulse.customer.ProfileBatch;
import com.retailpulse.customer.ProfileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties={"retailpulse.customer.enabled=true","retailpulse.clickhouse.password=test"})
@AutoConfigureMockMvc
@EnabledIfEnvironmentVariable(named="CUSTOMER_JDBC_URL",matches=".+")
class CustomerHttpTest {
    @Autowired MockMvc mvc;
    @Autowired ProfileStore store;
    @Test void customerEndpointsExposeBatchAndValidateRequests() throws Exception {
        store.initialize();
        String dataset=UUID.randomUUID().toString();
        var batch=new ProfileBatch(UUID.randomUUID().toString(),dataset,"source","rule",LocalDate.parse("2011-07-01"),180);
        store.publish(batch,List.of(new Profile("42","UK",3,2,new BigDecimal("20"),BigDecimal.ZERO,"A",3,2,2,"REPEAT_CUSTOMER")));
        mvc.perform(get("/api/customers").param("dataset",dataset)).andExpect(status().isOk())
                .andExpect(jsonPath("$.batch.observation").value("2011-07-01"))
                .andExpect(jsonPath("$.items[0].purchaseAmount").value("20.000000"));
        mvc.perform(get("/api/customers/42").param("dataset",dataset).param("batch",batch.id()))
                .andExpect(status().isOk()).andExpect(jsonPath("$.customerId").value("42"));
        mvc.perform(get("/api/customers/missing").param("dataset",dataset).param("batch",batch.id())).andExpect(status().isNotFound());
        mvc.perform(get("/api/customers").param("limit","101")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/customers").param("segment","INVALID")).andExpect(status().isBadRequest());
        mvc.perform(get("/customers.html")).andExpect(status().isOk());
    }
}
