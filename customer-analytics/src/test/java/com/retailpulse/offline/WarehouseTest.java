package com.retailpulse.offline;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "RETAILPULSE_SPARK_TESTS", matches = "true")
class WarehouseTest {
    @TempDir Path root;

    @Test
    void rulesAndPublicationSurviveRepeatAndIncompleteBuild() throws Exception {
        Path batch = Files.createDirectories(root.resolve("batch"));
        Files.writeString(batch.resolve("source.xlsx"), "deterministic-source");
        Files.write(batch.resolve("rows.csv"), List.of(String.join(",", RetailIngestor.FIELDS),
                "r2,2,same,100,A,Gift,2,2011-01-31T23:59:00,1.25,1,UK",
                "r3,3,same,100,A,Gift,2,2011-01-31T23:59:00,1.25,1,UK",
                "r4,4,other,100,A,Gift,1,2011-01-31T23:59:00,3,1,UK",
                "r5,5,cancel,C101,A,Gift,-1,2011-02-01T00:00:00,1.25,1,UK",
                "r6,6,anon,102,B,Box,1,2011-02-01T00:00:00,4,,UK",
                "r7,7,neg,103,A,Gift,-1,2011-02-01T00:00:00,1,1,UK",
                "r8,8,zero,104,A,Gift,1,2011-02-01T00:00:00,0,1,UK",
                "r9,9,date,105,A,Gift,1,invalid,2,1,UK"));
        Artifacts.publish(batch.resolve("manifest.json"), new SourceManifest("fixture", "fixture", "fixture",
                "fixture", Artifacts.sha256(batch.resolve("source.xlsx")), Artifacts.sha256(batch.resolve("rows.csv")),
                "Online Retail", 8, "GBP", "unspecified local wall time", 2));
        try (var spark = SparkSession.builder().appName("warehouse-test").enableHiveSupport().getOrCreate()) {
            var warehouse = new Warehouse(spark, root);
            String release = warehouse.build(batch);
            try {
                var report = warehouse.report(release);
                assertEquals(8, report.path("source").path("rows").asInt());
                assertEquals(2, report.path("purchases").path("orders").asInt());
                assertEquals("9.500000", report.path("purchases").path("gross_amount").asText());
                assertEquals("-1.250000", report.path("cancellations").path("signed_amount").asText());
                assertEquals(1, report.path("dimensions").path("customers").asInt());
                assertEquals(2, report.path("dimensions").path("products").asInt());
                assertEquals(3, spark.sql("SELECT * FROM transaction_audit WHERE disposition='QUARANTINED'").count());
                assertEquals("r2", spark.sql("SELECT duplicate_of FROM transaction_audit WHERE source_row_id='r3'").first().getString(0));
                assertEquals(release, warehouse.build(batch));
                assertEquals(report, warehouse.report(release));
                spark.sql("DROP TABLE dim_product");
                assertThrows(IllegalStateException.class, () -> warehouse.report(release));
                Files.delete(root.resolve("releases").resolve(release).resolve("manifest.json"));
                assertEquals(release, warehouse.build(batch));
                assertEquals(report, warehouse.report(release));
                assertTrue(warehouse.partitionPlan(release, "2011-01").contains("PartitionFilters"));
            } finally {
                spark.catalog().setCurrentDatabase("default");
                spark.sql("DROP DATABASE IF EXISTS retail_" + release + " CASCADE");
            }
        }
    }
}
