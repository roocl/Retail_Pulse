package com.retailpulse.offline;

import com.retailpulse.customer.Profile;
import com.retailpulse.customer.ProfileBatch;
import com.retailpulse.customer.ProfileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="CUSTOMER_JDBC_URL", matches=".+")
class ProfileStoreTest {
    @Test void publishIsAtomicRepeatableAndPaginationKeepsItsBatch() {
        var source = new DriverManagerDataSource(System.getenv("CUSTOMER_JDBC_URL"), "customer", System.getenv("CUSTOMER_MYSQL_PASSWORD"));
        var store = new ProfileStore(source);
        store.initialize();
        String dataset = UUID.randomUUID().toString();
        var first = new ProfileBatch(UUID.randomUUID().toString(), dataset, "source-release", "rule", LocalDate.parse("2011-07-01"), 180);
        var a = new Profile("1", "UK", 30, 1, new BigDecimal("30"), new BigDecimal("-5"), "A", 3, 1, 1, "SINGLE_PURCHASE");
        var b = new Profile("2", "UK", 10, 2, new BigDecimal("50"), BigDecimal.ZERO, "B", 3, 2, 2, "REPEAT_CUSTOMER");
        store.publish(first, List.of(a,b));
        store.publish(first, List.of(a,b));
        var page = store.list(dataset, null, null, null, null, 1);
        assertEquals(first.id(), page.batch().id());
        assertEquals("1", page.items().get(0).customerId());
        assertTrue(page.hasMore());
        var second = new ProfileBatch(UUID.randomUUID().toString(), dataset, "source-release", "rule2", first.observation(), 180);
        assertThrows(RuntimeException.class, () -> store.publish(second, List.of(a,a)));
        assertEquals(first.id(), store.list(dataset, null, null, null, null, 10).batch().id());
        store.publish(second, List.of(a));
        assertEquals("2", store.list(dataset, first.id(), "1", null, null, 1).items().get(0).customerId());
        assertTrue(store.find(dataset, second.id(), "2").isEmpty());
        assertThrows(RuntimeException.class, () -> store.publish(first, List.of(a)));
        assertEquals(1, store.list(dataset, first.id(), null, "REPEAT_CUSTOMER", null, 10).items().size());
    }
}
