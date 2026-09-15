package com.retailpulse.offline;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RETAILPULSE_SPARK_TESTS",matches="true")
class RankingMetricsTest {
    @Test void metricsUseFixedContactFractionAndKnownRankedLabels() throws Exception {
        try(var spark=SparkSession.builder().appName("ranking-metrics-test").getOrCreate()) {
            var scores=spark.sql("""
                SELECT * FROM VALUES ('1','2011-10-01',0.9D,1.0D),('2','2011-10-01',0.8D,1.0D),
                ('3','2011-10-01',0.7D,0.0D),('4','2011-10-01',0.4D,0.0D),('5','2011-10-01',0.1D,0.0D)
                AS t(customer_id,observation,score,label)
                """);
            var metrics=RankingMetrics.evaluate(scores,0.2);
            assertEquals(5,metrics.samples());assertEquals(2,metrics.positives());assertEquals(1,metrics.k());
            assertEquals(1.0,metrics.prAuc(),1e-9);assertEquals(1.0,metrics.precisionAtK(),1e-9);
            assertEquals(0.5,metrics.recallAtK(),1e-9);assertEquals(2.5,metrics.liftAtK(),1e-9);
            assertThrows(IllegalArgumentException.class,()->RankingMetrics.evaluate(scores,0));
        }
    }
}
