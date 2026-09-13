package com.retailpulse.offline;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.ZipInputStream;

@Command(name = "retailpulse-offline", mixinStandardHelpOptions = true)
public final class OfflineMain implements Callable<Integer> {
    enum Action { DOWNLOAD, INGEST, BUILD, REPORT, PLAN, VERSION }
    @Option(names = "--root") private Path root = Path.of("/data/retail");
    @Parameters(index = "0") private Action action;
    @Parameters(index = "1..*", arity = "0..2") private List<String> arguments = new ArrayList<>();

    public static void main(String[] arguments) {
        System.exit(new CommandLine(new OfflineMain()).setCaseInsensitiveEnumValuesAllowed(true).execute(arguments));
    }

    @Override public Integer call() throws Exception {
        int required = switch (action) { case DOWNLOAD, VERSION -> 0; case INGEST, BUILD, REPORT -> 1; case PLAN -> 2; };
        if (arguments.size() != required) throw new IllegalArgumentException(action + " requires " + required + " argument(s)");
        if (action == Action.VERSION) {
            System.out.println(Warehouse.implementationVersion());
            return 0;
        }
        if (action == Action.DOWNLOAD) {
            Path incoming = Files.createDirectories(root.resolve("incoming"));
            Path temporary = Files.createTempDirectory(incoming, "download-");
            try {
                var client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(30)).build();
                var request = HttpRequest.newBuilder(URI.create(SourceManifest.DOWNLOAD_URL)).timeout(Duration.ofSeconds(90)).GET().build();
                var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                try (var body = response.body()) {
                    if (response.statusCode() != 200) throw new IllegalStateException("Download HTTP " + response.statusCode());
                    Files.copy(body, temporary.resolve("source.zip"));
                }
                Path workbook = temporary.resolve("Online Retail.xlsx");
                try (var zip = new ZipInputStream(Files.newInputStream(temporary.resolve("source.zip")))) {
                    for (var entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                        if (entry.getName().equals("Online Retail.xlsx")) { Files.copy(zip, workbook); break; }
                    }
                }
                System.out.println(new RetailIngestor(root).ingest(workbook));
            } finally { FileUtils.deleteDirectory(temporary.toFile()); }
            return 0;
        }
        if (action == Action.INGEST) {
            System.out.println(new RetailIngestor(root).ingest(Path.of(arguments.get(0))));
            return 0;
        }
        try (var spark = SparkSession.builder().appName("retailpulse-offline").enableHiveSupport().getOrCreate()) {
            var warehouse = new Warehouse(spark, root);
            switch (action) {
                case BUILD -> System.out.println(warehouse.build(Path.of(arguments.get(0))));
                case REPORT -> System.out.println(Artifacts.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(warehouse.report(arguments.get(0))));
                case PLAN -> System.out.println(warehouse.partitionPlan(arguments.get(0), arguments.get(1)));
                default -> throw new IllegalStateException("Unexpected action");
            }
        }
        return 0;
    }
}
