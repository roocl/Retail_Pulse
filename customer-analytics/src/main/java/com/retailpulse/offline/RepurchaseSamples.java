package com.retailpulse.offline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.time.LocalDate;

public final class RepurchaseSamples {
    private final ExperimentPlan plan;
    public RepurchaseSamples(ExperimentPlan plan) {this.plan=plan;}
    public Dataset<Row> at(Dataset<Row> facts, LocalDate observation, LocalDate coverageStart,
                           LocalDate coverageEndExclusive) throws Exception {
        if(!plan.labelAvailable(observation,coverageStart,coverageEndExclusive))
            throw new IllegalArgumentException("Incomplete history or future label interval");
        new Profiles().features(facts,observation,plan.historyDays()).createOrReplaceTempView("historical_features");
        facts.createOrReplaceTempView("label_source");
        return facts.sparkSession().sql(Artifacts.sql("repurchase_samples")
                .replace("${observation}",observation.toString())
                .replace("${label_end}",observation.plusDays(plan.horizonDays()).toString()));
    }
}
