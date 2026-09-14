package com.retailpulse.offline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.time.LocalDate;

public final class Profiles {
    public Dataset<Row> calculate(Dataset<Row> facts, LocalDate observation, int windowDays) throws Exception {
        if (windowDays < 1 || windowDays > 366) throw new IllegalArgumentException("Window must be between 1 and 366 days");
        facts.createOrReplaceTempView("profile_source");
        return facts.sparkSession().sql(Artifacts.sql("profiles")
                .replace("${observation}", observation.toString())
                .replace("${window_start}", observation.minusDays(windowDays).toString()));
    }
}
