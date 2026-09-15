package com.retailpulse.offline;

import com.retailpulse.customer.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="CUSTOMER_JDBC_URL", matches=".+")
class ScoreStoreTest {
    @Test void scoresPublishAtomicallyWithinTheirProfileBatch() {
        var store=new ProfileStore(new DriverManagerDataSource(System.getenv("CUSTOMER_JDBC_URL"),"customer",System.getenv("CUSTOMER_MYSQL_PASSWORD")));
        store.initialize();
        String scope=UUID.randomUUID().toString();
        var profile=new ProfileBatch(scope,scope,"source","rule",LocalDate.parse("2011-12-10"),180);
        store.publish(profile,List.of(new Profile("1","UK",1,1,BigDecimal.TEN,BigDecimal.ZERO,"A",1,1,1,"SINGLE_PURCHASE")));
        var first=new ScoreBatch(UUID.randomUUID().toString(),scope,"model","LOGISTIC_REGRESSION",30);
        var value=new CustomerScore("1",new BigDecimal("0.75"));
        store.publishScores(first,List.of(value));
        store.publishScores(first,List.of(value));
        assertEquals(value,store.prediction(scope,scope,"1").orElseThrow().score());
        var second=new ScoreBatch(UUID.randomUUID().toString(),scope,"model2","RANDOM_FOREST",30);
        assertThrows(RuntimeException.class,()->store.publishScores(second,List.of(value,value)));
        assertEquals(first,store.prediction(scope,scope,"1").orElseThrow().batch());
        assertThrows(RuntimeException.class,()->store.publishScores(second,List.of(new CustomerScore("missing",BigDecimal.ONE))));
        store.publishScores(second,List.of(new CustomerScore("1",BigDecimal.ONE)));
        store.publishScores(first,List.of(value));
        assertEquals(second,store.prediction(scope,scope,"1").orElseThrow().batch());
        assertThrows(RuntimeException.class,()->store.publishScores(first,List.of(new CustomerScore("1",BigDecimal.ZERO))));
        assertTrue(store.prediction("wrong-dataset",scope,"1").isEmpty());
    }
}
