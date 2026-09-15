package com.retailpulse.offline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.time.LocalDate;

public final class Profiles {
    public Dataset<Row> features(Dataset<Row> facts, LocalDate observation, int windowDays) throws Exception {
        if (windowDays < 1 || windowDays > 366) throw new IllegalArgumentException("Window must be between 1 and 366 days");
        facts.createOrReplaceTempView("profile_source");
        return facts.sparkSession().sql(Artifacts.sql("customer_features")
                .replace("${observation}", observation.toString())
                .replace("${window_start}", observation.minusDays(windowDays).toString()));
    }
    public Dataset<Row> calculate(Dataset<Row> facts, LocalDate observation, int windowDays) throws Exception {
        features(facts,observation,windowDays).createOrReplaceTempView("customer_features");
        return facts.sparkSession().sql(Artifacts.sql("profiles"));
    }
    public static String version() throws Exception {
        try(var code=Profiles.class.getResourceAsStream("Profiles.class")) {
            return Artifacts.sha256(Artifacts.sql("customer_features")+Artifacts.sql("profiles")
                    +java.util.HexFormat.of().formatHex(code.readAllBytes()));
        }
    }
}
