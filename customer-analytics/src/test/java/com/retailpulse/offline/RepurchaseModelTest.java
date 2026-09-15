package com.retailpulse.offline;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named="RETAILPULSE_SPARK_TESTS",matches="true")
class RepurchaseModelTest {
    @TempDir Path root;
    @Test void trainedPipelinePersistsItsPreprocessingAndScores() throws Exception {
        try(var spark=SparkSession.builder().appName("repurchase-model-test").getOrCreate()) {
            var training=spark.sql("""
                SELECT CAST(id AS STRING) customer_id,'2011-06-01' observation,
                CAST(CASE WHEN id%2=0 THEN 2 ELSE 90 END AS INT) recency_days,
                CAST(CASE WHEN id%2=0 THEN 8 ELSE 1 END AS BIGINT) orders,
                CAST(id+100 AS DECIMAL(30,6)) purchase_amount,CAST(0 AS DECIMAL(30,6)) cancellation_amount,
                CAST(CASE WHEN id%2=0 THEN 1 ELSE 0 END AS DOUBLE) label
                FROM range(40)
                """);
            for(var candidate:RepurchaseModel.candidates().stream().filter(c->c.id().equals("logistic-0.1") || c.id().equals("forest-3")).toList()) {
                var model=RepurchaseModel.fit(training,candidate,42);
                var before=model.score(training).select("customer_id","score").orderBy("customer_id").collectAsList();
                var path=root.resolve(candidate.id());model.save(path);
                var after=RepurchaseModel.load(path).score(training).select("customer_id","score").orderBy("customer_id").collectAsList();
                assertEquals(before,after);
                assertEquals(40,after.size());
                assertTrue(before.get(0).getDouble(1)>0.5);
                assertTrue(before.get(1).getDouble(1)<0.5);
            }
        }
    }
}
