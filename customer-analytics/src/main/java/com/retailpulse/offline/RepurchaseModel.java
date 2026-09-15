package com.retailpulse.offline;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.nio.file.Path;
import java.util.List;
import static org.apache.spark.sql.functions.col;

public final class RepurchaseModel {
    public enum Algorithm { LOGISTIC_REGRESSION, RANDOM_FOREST }
    public record Candidate(String id,Algorithm algorithm,double regularization,int maxDepth) {}
    private final PipelineModel pipeline;
    private RepurchaseModel(PipelineModel pipeline) {this.pipeline=pipeline;}
    public static List<Candidate> candidates() {
        return List.of(new Candidate("logistic-0.01",Algorithm.LOGISTIC_REGRESSION,0.01,0),
                new Candidate("logistic-0.1",Algorithm.LOGISTIC_REGRESSION,0.1,0),
                new Candidate("forest-3",Algorithm.RANDOM_FOREST,0,3),new Candidate("forest-5",Algorithm.RANDOM_FOREST,0,5));
    }
    public static RepurchaseModel fit(Dataset<Row> training,Candidate candidate,long seed) {
        var preprocessing=new SQLTransformer().setStatement("""
                SELECT *, CAST(recency_days AS DOUBLE) AS feature_recency,
                LOG1P(CAST(orders AS DOUBLE)) AS feature_orders,
                LOG1P(CAST(purchase_amount AS DOUBLE)) AS feature_purchase,
                LOG1P(-CAST(cancellation_amount AS DOUBLE)) AS feature_cancellation
                FROM __THIS__
                """);
        var assembler=new VectorAssembler().setInputCols(new String[]{"feature_recency","feature_orders","feature_purchase","feature_cancellation"})
                .setOutputCol("unscaled_features").setHandleInvalid("error");
        var scaler=new StandardScaler().setInputCol("unscaled_features").setOutputCol("features").setWithMean(true).setWithStd(true);
        PipelineStage estimator=switch(candidate.algorithm()) {
            case LOGISTIC_REGRESSION -> new LogisticRegression().setFeaturesCol("features").setLabelCol("label")
                    .setMaxIter(60).setRegParam(candidate.regularization()).setStandardization(false);
            case RANDOM_FOREST -> new RandomForestClassifier().setFeaturesCol("features").setLabelCol("label")
                    .setSeed(seed).setNumTrees(40).setMaxDepth(candidate.maxDepth());
        };
        return new RepurchaseModel(new Pipeline().setStages(new PipelineStage[]{preprocessing,assembler,scaler,estimator}).fit(training));
    }
    public Dataset<Row> score(Dataset<Row> features) {
        return pipeline.transform(features).withColumn("score",org.apache.spark.ml.functions.vector_to_array(col("probability"),"float64").getItem(1));
    }
    public static Dataset<Row> baseline(Dataset<Row> features) {
        return features.withColumn("score",col("recency_days").cast("double").multiply(-1.0));
    }
    public void save(Path path) throws Exception {pipeline.write().save(path.toString());}
    public static RepurchaseModel load(Path path) {return new RepurchaseModel(PipelineModel.load(path.toString()));}
}
