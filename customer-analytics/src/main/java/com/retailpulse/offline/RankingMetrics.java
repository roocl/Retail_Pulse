package com.retailpulse.offline;

import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Comparator;

public record RankingMetrics(long samples,long positives,double prevalence,Double prAuc,int k,
                             double precisionAtK,Double recallAtK,Double liftAtK) {
    public static RankingMetrics evaluate(Dataset<Row> predictions,double fraction) {
        if(!Double.isFinite(fraction) || fraction<=0 || fraction>1) throw new IllegalArgumentException("Invalid contact fraction");
        var rows=predictions.select("customer_id","observation","score","label").collectAsList();
        if(rows.isEmpty()) throw new IllegalArgumentException("Empty evaluation sample");
        for(var row:rows) if(!Double.isFinite(row.getDouble(2)) || (row.getDouble(3)!=0 && row.getDouble(3)!=1))
            throw new IllegalArgumentException("Nonfinite score or invalid binary label");
        rows.sort(Comparator.<Row>comparingDouble(r->r.getDouble(2)).reversed().thenComparing(r->r.getString(1)).thenComparing(r->r.getString(0)));
        long positives=rows.stream().filter(r->r.getDouble(3)==1).count();
        int k=(int)Math.ceil(rows.size()*fraction);
        long selected=rows.subList(0,k).stream().filter(r->r.getDouble(3)==1).count();
        double prevalence=(double)positives/rows.size();
        double precision=(double)selected/k;
        Double auc=positives==0 ? null : new BinaryClassificationEvaluator().setRawPredictionCol("score")
                .setLabelCol("label").setMetricName("areaUnderPR").setNumBins(0).evaluate(predictions);
        return new RankingMetrics(rows.size(),positives,prevalence,auc,k,precision,
                positives==0?null:(double)selected/positives,positives==0?null:precision/prevalence);
    }
}
