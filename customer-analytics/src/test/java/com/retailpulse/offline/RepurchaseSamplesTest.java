package com.retailpulse.offline;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RETAILPULSE_SPARK_TESTS",matches="true")
class RepurchaseSamplesTest {
    @Test void futurePurchasesChangeLabelsButNeverHistoricalFeatures() throws Exception {
        try(var spark=SparkSession.builder().appName("repurchase-samples-test").getOrCreate()) {
            var facts=spark.sql("""
                SELECT * FROM VALUES
                ('a','1','A','UK','2011-05-20 12:00:00','PURCHASE',10.0,'1'),
                ('b','2','B','UK','2011-05-21 12:00:00','PURCHASE',20.0,'2'),
                ('c','3','C','FR','2011-06-01 00:00:00','PURCHASE',999.0,'1'),
                ('d','4','D','FR','2011-07-01 00:00:00','PURCHASE',999.0,'2'),
                ('e','5','A','UK','2011-06-15 00:00:00','CANCELLATION',-20.0,'2')
                AS t(source_row_id,invoice_no,stock_code,country,invoice_time,disposition,signed_amount,customer_id)
                """);
            var sampler=new RepurchaseSamples(ExperimentPlan.standard());
            var observation=LocalDate.parse("2011-06-01");
            var labeled=sampler.at(facts,observation,LocalDate.parse("2010-12-01"),LocalDate.parse("2011-08-01"));
            var rows=labeled.orderBy("customer_id").collectAsList();
            assertEquals(2,rows.size());
            assertEquals(1.0,rows.get(0).<Double>getAs("label"));
            assertEquals(0.0,rows.get(1).<Double>getAs("label"));
            assertEquals(0,new java.math.BigDecimal("10").compareTo(rows.get(0).getAs("purchase_amount")));
            facts.createOrReplaceTempView("sample_fixture");
            var history=spark.sql("SELECT * FROM sample_fixture WHERE invoice_time<'2011-06-01'");
            var before=new Profiles().features(history,observation,180).orderBy("customer_id").collectAsList();
            var after=new Profiles().features(facts,observation,180).orderBy("customer_id").collectAsList();
            assertEquals(before,after);
            assertThrows(IllegalArgumentException.class,()->sampler.at(facts,observation,LocalDate.parse("2010-12-01"),LocalDate.parse("2011-06-30")));
        }
    }
}
