package com.retailpulse.offline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public final class Warehouse {
    private static final List<String> TABLES = List.of("raw_transactions", "transaction_audit", "transaction_facts", "dim_customer", "dim_product");
    private final SparkSession spark;
    private final Path root;

    public Warehouse(SparkSession spark, Path root) throws IOException {
        this.spark = spark;
        this.root = Files.createDirectories(root.toAbsolutePath().normalize());
    }

    public String build(Path batch) throws Exception {
        var source = Artifacts.JSON.readValue(batch.resolve("manifest.json").toFile(), SourceManifest.class);
        if (!Artifacts.sha256(batch.resolve("source.xlsx")).equals(source.sourceSha256())
                || !Artifacts.sha256(batch.resolve("rows.csv")).equals(source.rowsSha256()))
            throw new IOException("Input checksum mismatch");
        String version = implementationVersion();
        String release = Artifacts.sha256(source.sourceSha256() + ":" + source.rowsSha256() + ":" + version);
        String database = "retail_" + release;
        Path output = root.resolve("releases").resolve(release);
        try (var ignored = new Artifacts.Lock(root.resolve("warehouse.lock"))) {
            if (Files.exists(output.resolve("manifest.json"))) { report(release); return release; }
            Files.createDirectories(output);
            spark.sql("CREATE DATABASE IF NOT EXISTS " + database);
            spark.catalog().setCurrentDatabase(database);
            String schema = Arrays.stream(RetailIngestor.FIELDS).map(name -> name + " STRING").collect(Collectors.joining(","));
            var raw = spark.read().schema(schema).option("header", true).option("multiLine", true)
                    .option("escape", "\"").option("mode", "FAILFAST").csv(batch.resolve("rows.csv").toString());
            write("raw_transactions", raw, database);
            spark.table("raw_transactions").createOrReplaceTempView("source_rows");
            write("transaction_audit", spark.sql(Artifacts.sql("classify")), database);
            write("transaction_facts", spark.sql("SELECT * FROM transaction_audit WHERE disposition IN ('PURCHASE','CANCELLATION')"), database);
            for (String table : List.of("dim_customer", "dim_product")) write(table, spark.sql(Artifacts.sql(table)), database);
            spark.sql("ANALYZE TABLE transaction_facts COMPUTE STATISTICS");
            Artifacts.publish(output.resolve("quality.json"), measure(source));
            Artifacts.publish(output.resolve("manifest.json"), new ReleaseManifest(release, database,
                    source.sourceSha256(), version, Artifacts.sha256(output.resolve("quality.json"))));
        }
        return release;
    }

    private void write(String table, Dataset<Row> data, String database) {
        var writer = data.write().mode("overwrite").format("parquet").option("path", "/warehouse/" + database + ".db/" + table);
        if (table.equals("transaction_audit") || table.equals("transaction_facts")) writer = writer.partitionBy("transaction_month");
        writer.saveAsTable(table);
    }

    private ObjectNode measure(SourceManifest source) throws IOException {
        var totals = row(spark.sql(Artifacts.sql("source_totals")).first());
        var audit = row(spark.sql(Artifacts.sql("audit_totals")).first());
        if (!totals.equals(audit) || totals.path("rows").asLong() != source.rows()
                || totals.path("distinct_rows").asLong() != source.rows())
            throw new IllegalStateException("Source/audit reconciliation failed");
        ObjectNode report = Artifacts.JSON.createObjectNode();
        report.set("source", Artifacts.JSON.valueToTree(source));
        report.set("input_profile", row(spark.sql(Artifacts.sql("input_profile")).first()));
        var reconciliation = report.putObject("reconciliation");
        reconciliation.set("raw", totals);
        reconciliation.set("audit", audit);
        reconciliation.put("passed", true);
        for (String query : List.of("purchases", "cancellations", "dimension_join"))
            report.set(query, row(spark.sql(Artifacts.sql(query)).first()));
        var buckets = report.putArray("dispositions");
        for (Row record : spark.sql(Artifacts.sql("dispositions")).collectAsList()) buckets.add(row(record));
        report.putObject("dimensions").put("customers", spark.table("dim_customer").count()).put("products", spark.table("dim_product").count());
        long retained = report.path("purchases").path("lines").asLong() + report.path("cancellations").path("lines").asLong();
        if (report.path("dimension_join").path("fact_rows").asLong() != retained
                || report.path("dimension_join").path("product_matches").asLong() != retained)
            throw new IllegalStateException("Dimension join reconciliation failed");
        return report;
    }

    private static ObjectNode row(Row row) {
        var result = Artifacts.JSON.createObjectNode();
        String[] names = row.schema().fieldNames();
        for (int i = 0; i < names.length; i++) {
            Object value = row.get(i);
            result.set(names[i], Artifacts.JSON.valueToTree(value instanceof BigDecimal number ? number.toPlainString() : value));
        }
        return result;
    }

    private Path published(String release) throws Exception {
        if (!release.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Expected a release SHA-256");
        Path output = root.resolve("releases").resolve(release);
        var manifest = Artifacts.JSON.readValue(output.resolve("manifest.json").toFile(), ReleaseManifest.class);
        if (!manifest.release().equals(release) || !manifest.database().equals("retail_" + release)
                || !Artifacts.sha256(output.resolve("quality.json")).equals(manifest.qualitySha256()))
            throw new IllegalStateException("Published release integrity check failed");
        spark.catalog().setCurrentDatabase(manifest.database());
        for (String table : TABLES) if (!spark.catalog().tableExists(table))
            throw new IllegalStateException("Published table is missing: " + table);
        return output;
    }

    public JsonNode report(String release) throws Exception {
        var expected = Artifacts.JSON.readTree(published(release).resolve("quality.json").toFile());
        var source = Artifacts.JSON.treeToValue(expected.path("source"), SourceManifest.class);
        var actual = Artifacts.JSON.readTree(measure(source).toString());
        if (!expected.equals(actual)) throw new IllegalStateException("Published data no longer matches its quality report");
        return expected;
    }

    public String partitionPlan(String release, String month) throws Exception {
        published(release);
        if (!month.matches("\\d{4}-(0[1-9]|1[0-2])")) throw new IllegalArgumentException("Expected month YYYY-MM");
        return spark.sql("EXPLAIN FORMATTED SELECT SUM(signed_amount) FROM transaction_facts WHERE transaction_month='" + month + "'")
                .collectAsList().stream().map(record -> record.getString(0)).collect(Collectors.joining("\n"));
    }

    public static String implementationVersion() throws Exception {
        Path location = Path.of(Warehouse.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        var entries = new TreeMap<String, byte[]>();
        if (Files.isDirectory(location)) {
            try (var paths = Files.walk(location)) {
                for (Path path : paths.filter(Files::isRegularFile).toList()) {
                    String name = location.relativize(path).toString().replace('\\', '/');
                    if (name.endsWith(".class") || name.endsWith(".sql")) entries.put(name, Files.readAllBytes(path));
                }
            }
        } else {
            try (var jar = new JarFile(location.toFile())) {
                for (var entry : jar.stream().filter(e -> e.getName().endsWith(".class") || e.getName().endsWith(".sql")).toList())
                    try (var input = jar.getInputStream(entry)) { entries.put(entry.getName(), input.readAllBytes()); }
            }
        }
        var digest = Artifacts.digest();
        for (var entry : entries.entrySet()) {
            digest.update(entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            digest.update(entry.getValue());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private record ReleaseManifest(String release, String database, String sourceSha256,
                                   String transformVersion, String qualitySha256) { }
}
