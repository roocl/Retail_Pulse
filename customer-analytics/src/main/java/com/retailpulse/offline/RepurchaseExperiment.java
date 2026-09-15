package com.retailpulse.offline;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class RepurchaseExperiment {
    public record Manifest(String id,String sourceRelease,String featureVersion,String implementationVersion,
                           ExperimentPlan plan,RepurchaseModel.Candidate selected,LocalDate coverageStart,
                           LocalDate coverageEndExclusive,Map<String,String> files) {}
    public record Evaluation(String candidate,String period,RankingMetrics metrics) {}
    private final SparkSession spark;
    private final Path root;
    public RepurchaseExperiment(SparkSession spark,Path root) {this.spark=spark;this.root=root;}

    public String train(String sourceRelease) throws Exception {
        var quality=new Warehouse(spark,root).report(sourceRelease);
        var plan=ExperimentPlan.standard();
        String featureVersion=Profiles.version(),implementation=Warehouse.implementationVersion();
        String id=Artifacts.sha256(sourceRelease+":"+implementation+":"+Artifacts.JSON.writeValueAsString(plan));
        Path models=Files.createDirectories(root.resolve("models"));
        try(var lock=new Artifacts.Lock(root.resolve("models.lock"))) {
            if(Files.exists(models.resolve(id))) {manifest(id);return id;}
            Path temporary=Files.createTempDirectory(models,"training-");
            Dataset<Row> facts=null,training=null,validation=null;
            try {
                var start=LocalDate.parse(quality.path("input_profile").path("first_local_time").asText().substring(0,10));
                var end=LocalDate.parse(quality.path("input_profile").path("last_local_time").asText().substring(0,10));
                facts=spark.table("transaction_facts").cache();
                var dates=new ArrayList<LocalDate>();dates.addAll(plan.training());dates.addAll(plan.validation());dates.addAll(plan.test());
                var sampler=new RepurchaseSamples(plan);
                Dataset<Row> samples=null;
                for(var date:dates) {
                    var current=sampler.at(facts,date,start,end);
                    samples=samples==null?current:samples.unionByName(current);
                }
                samples.write().parquet(temporary.resolve("samples").toString());
                facts.unpersist();facts=null;
                spark.read().parquet(temporary.resolve("samples").toString()).createOrReplaceTempView("model_samples");
                training=slice(plan.training()).cache();validation=slice(plan.validation()).cache();
                for(var set:List.of(training,validation)) if(set.select("label").distinct().count()!=2)
                    throw new IllegalStateException("Training and validation must contain both classes");
                var candidates=new LinkedHashMap<String,RepurchaseModel>();
                var validationResults=new ArrayList<Evaluation>();
                validationResults.add(new Evaluation("recency-baseline","validation",RankingMetrics.evaluate(RepurchaseModel.baseline(validation),plan.contactFraction())));
                var measured=new LinkedHashMap<RepurchaseModel.Candidate,RankingMetrics>();
                for(var candidate:RepurchaseModel.candidates()) {
                    var model=RepurchaseModel.fit(training,candidate,plan.seed());
                    candidates.put(candidate.id(),model);
                    var metrics=RankingMetrics.evaluate(model.score(validation),plan.contactFraction());
                    measured.put(candidate,metrics);
                    validationResults.add(new Evaluation(candidate.id(),"validation",metrics));
                }
                var ranking=Comparator.<RepurchaseModel.Candidate>comparingDouble(c->measured.get(c).prAuc()).reversed()
                        .thenComparing(RepurchaseModel.Candidate::id);
                var selected=measured.keySet().stream().sorted(ranking).findFirst().orElseThrow();
                var finalists=new ArrayList<RepurchaseModel.Candidate>();
                for(var algorithm:RepurchaseModel.Algorithm.values()) finalists.add(measured.keySet().stream()
                        .filter(c->c.algorithm()==algorithm).sorted(ranking).findFirst().orElseThrow());
                Artifacts.publish(temporary.resolve("selection.json"),Map.of("selected",selected,"finalists",finalists,"validation",validationResults));
                var model=candidates.get(selected.id());model.save(temporary.resolve("model"));
                var reloaded=RepurchaseModel.load(temporary.resolve("model"));
                var before=model.score(validation).select("customer_id","observation","score").orderBy("customer_id","observation").collectAsList();
                var after=reloaded.score(validation).select("customer_id","observation","score").orderBy("customer_id","observation").collectAsList();
                if(!before.equals(after)) throw new IllegalStateException("Reloaded model changed scores");
                var testResults=new ArrayList<Evaluation>();
                Dataset<Row> finalTest=slice(plan.test()).cache();
                try {
                    var periods=new LinkedHashMap<String,Dataset<Row>>();periods.put("test-all",finalTest);
                    for(var date:plan.test()) periods.put(date.toString(),slice(List.of(date)));
                    for(var period:periods.entrySet()) {
                        testResults.add(new Evaluation("recency-baseline",period.getKey(),RankingMetrics.evaluate(RepurchaseModel.baseline(period.getValue()),plan.contactFraction())));
                        for(var candidate:finalists) testResults.add(new Evaluation(candidate.id(),period.getKey(),
                                RankingMetrics.evaluate(candidates.get(candidate.id()).score(period.getValue()),plan.contactFraction())));
                    }
                    var scored=reloaded.score(finalTest).select("customer_id","observation","score","label","recency_days","orders","purchase_amount","cancellation_amount");
                    scored.orderBy("observation","customer_id").coalesce(1).write().option("header",true).csv(temporary.resolve("test_predictions").toString());
                    var errors=new ArrayList<Map<String,Object>>();
                    scored.createOrReplaceTempView("evaluated_predictions");
                    for(String clause:List.of("label=0 ORDER BY score DESC,observation,customer_id","label=1 ORDER BY score ASC,observation,customer_id")) {
                        for(var row:spark.sql("SELECT * FROM evaluated_predictions WHERE "+clause+" LIMIT 5").collectAsList()) {
                            var example=new LinkedHashMap<String,Object>();
                            for(int i=0;i<row.size();i++)example.put(row.schema().fieldNames()[i],row.get(i));
                            errors.add(example);
                        }
                    }
                    Artifacts.publish(temporary.resolve("evaluation.json"),Map.of("plan",plan,"selected",selected,"validation",validationResults,
                            "test",testResults,"error_examples",errors,"reload_identical",true,"spark",spark.version(),"java",System.getProperty("java.version")));
                } finally {finalTest.unpersist();}
                var manifest=new Manifest(id,sourceRelease,featureVersion,implementation,plan,selected,start,end,checksums(temporary));
                Artifacts.publish(temporary.resolve("manifest.json"),manifest);
                Files.move(temporary,models.resolve(id),StandardCopyOption.ATOMIC_MOVE);
            } finally {
                if(training!=null)training.unpersist();if(validation!=null)validation.unpersist();if(facts!=null)facts.unpersist();
                if(Files.exists(temporary))FileUtils.deleteDirectory(temporary.toFile());
            }
        }
        return id;
    }

    public String score(String modelId,String dataset,String profileBatchId) throws Exception {
        var manifest=manifest(modelId);
        if(!manifest.featureVersion().equals(Profiles.version()))throw new IllegalStateException("Model feature implementation differs");
        var store=new com.retailpulse.customer.ProfileStore(new org.springframework.jdbc.datasource.DriverManagerDataSource(
                System.getenv("CUSTOMER_JDBC_URL"),"customer",System.getenv("CUSTOMER_MYSQL_PASSWORD")));
        store.initialize();
        var profile=store.list(dataset,profileBatchId,null,null,null,1).batch();
        var plan=manifest.plan();
        if(!profile.sourceRelease().equals(manifest.sourceRelease()) || profile.windowDays()!=plan.historyDays()
                || profile.observation().isBefore(plan.validation().get(plan.validation().size()-1).plusDays(plan.horizonDays())))
            throw new IllegalArgumentException("Profile source, history or scoring date is incompatible with model");
        new Warehouse(spark,root).report(profile.sourceRelease());
        var features=new Profiles().features(spark.table("transaction_facts"),profile.observation(),plan.historyDays());
        var scored=RepurchaseModel.load(root.resolve("models").resolve(modelId).resolve("model")).score(features);
        var values=scored.select("customer_id","score").collectAsList().stream().map(row->
                new com.retailpulse.customer.CustomerScore(row.getString(0),java.math.BigDecimal.valueOf(row.getDouble(1)))).toList();
        String id=Artifacts.sha256(modelId+":"+profileBatchId);
        var batch=new com.retailpulse.customer.ScoreBatch(id,profileBatchId,modelId,manifest.selected().algorithm().name(),plan.horizonDays());
        store.publishScores(batch,values);
        Path output=Files.createDirectories(root.resolve("scores").resolve(id));
        Artifacts.publish(output.resolve("batch.json"),Map.of("batch",batch,"observation",profile.observation(),"customers",values.size()));
        return id;
    }

    public com.fasterxml.jackson.databind.JsonNode evaluation(String modelId) throws Exception {
        manifest(modelId);
        return Artifacts.JSON.readTree(root.resolve("models").resolve(modelId).resolve("evaluation.json").toFile());
    }
    private Dataset<Row> slice(List<LocalDate> dates) {
        String values=dates.stream().map(d->"'"+d+"'").collect(Collectors.joining(","));
        return spark.sql("SELECT * FROM model_samples WHERE observation IN ("+values+")");
    }

    public Manifest manifest(String id) throws Exception {
        if(!id.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("Expected model SHA-256");
        Path directory=root.resolve("models").resolve(id);
        var manifest=Artifacts.JSON.readValue(directory.resolve("manifest.json").toFile(),Manifest.class);
        if(!manifest.id().equals(id) || !manifest.files().equals(checksums(directory)))
            throw new IllegalStateException("Model artifact integrity check failed");
        return manifest;
    }

    private static Map<String,String> checksums(Path directory) throws Exception {
        var files=new TreeMap<String,String>();
        try(var paths=Files.walk(directory)) {
            for(var path:paths.filter(Files::isRegularFile).toList()) {
                String name=directory.relativize(path).toString().replace('\\','/');
                if(!name.equals("manifest.json"))files.put(name,Artifacts.sha256(path));
            }
        }
        return files;
    }
}
