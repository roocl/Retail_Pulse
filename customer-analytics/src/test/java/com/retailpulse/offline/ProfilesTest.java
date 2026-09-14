package com.retailpulse.offline;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "RETAILPULSE_SPARK_TESTS", matches = "true")
class ProfilesTest {
    @Test void historicalProfilesExcludeFutureAndSeparateCancellations() throws Exception {
        try (var spark = SparkSession.builder().appName("profiles-test").getOrCreate()) {
            var facts = spark.sql("""
                SELECT * FROM VALUES
                ('a','1','A','Gift','UK','2011-06-01 12:00:00','PURCHASE',10.00,1),
                ('b','1','A','Gift','UK','2011-06-01 12:00:00','PURCHASE',20.00,2),
                ('c','2','A','Gift','UK','2011-06-02 12:00:00','CANCELLATION',-5.00,-1),
                ('d','3','B','Future','FR','2011-07-01 00:00:00','PURCHASE',999.00,1),
                ('e','4','A','Gift','UK','2011-06-01 12:00:00','PURCHASE',30.00,3)
                AS t(source_row_id,invoice_no,stock_code,description,country,invoice_time,disposition,signed_amount,quantity)
                """).withColumn("customer_id", org.apache.spark.sql.functions.expr("CASE WHEN source_row_id='e' THEN '2' ELSE '1' END"));
            var rows = new Profiles().calculate(facts, LocalDate.parse("2011-07-01"), 180).orderBy("customer_id").collectAsList();
            assertEquals(2, rows.size());
            var first = rows.get(0);
            assertEquals(30, ((Number) first.getAs("recency_days")).intValue());
            assertEquals(1L, ((Number) first.getAs("orders")).longValue());
            assertEquals(0, new java.math.BigDecimal("30").compareTo(first.getAs("purchase_amount")));
            assertEquals(0, new java.math.BigDecimal("-5").compareTo(first.getAs("cancellation_amount")));
            assertEquals("UK", first.getAs("country"));
            assertEquals("A", first.getAs("preferred_product"));
            assertEquals("SINGLE_PURCHASE", first.getAs("segment"));
            assertEquals(first.<Integer>getAs("m_score"), rows.get(1).<Integer>getAs("m_score"));
        }
    }
    @Test void windowIncludesStartAndExcludesEndAndAnonymousRows() throws Exception {
        try(var spark=SparkSession.builder().appName("profile-boundary-test").getOrCreate()) {
            var facts=spark.sql("""
                SELECT * FROM VALUES
                ('a','1','A','UK','2011-01-01 23:59:59','PURCHASE',100.00,'1'),
                ('b','2','B','UK','2011-01-02 00:00:00','PURCHASE',20.00,'1'),
                ('c','3','C','FR','2011-07-01 00:00:00','PURCHASE',999.00,'1'),
                ('d','4','D','UK','2011-06-01 00:00:00','PURCHASE',777.00,NULL)
                AS t(source_row_id,invoice_no,stock_code,country,invoice_time,disposition,signed_amount,customer_id)
                """);
            var calculator=new Profiles();
            var rows=calculator.calculate(facts,LocalDate.parse("2011-07-01"),180).collectAsList();
            assertEquals(1,rows.size());
            assertEquals(180,((Number)rows.get(0).getAs("recency_days")).intValue());
            assertEquals(0,new java.math.BigDecimal("20").compareTo(rows.get(0).getAs("purchase_amount")));
            assertEquals("B",rows.get(0).getAs("preferred_product"));
            assertEquals(0,calculator.calculate(facts,LocalDate.parse("2010-01-01"),180).count());
        }
    }}
