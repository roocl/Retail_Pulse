package com.retailpulse.offline;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public record ExperimentPlan(List<LocalDate> training, List<LocalDate> validation, List<LocalDate> test,
                             int historyDays, int horizonDays, long seed, double contactFraction) {
    public ExperimentPlan {
        training=List.copyOf(training);validation=List.copyOf(validation);test=List.copyOf(test);
        if(training.isEmpty() || validation.isEmpty() || test.isEmpty() || historyDays<1 || historyDays>366
                || horizonDays<1 || contactFraction<=0 || contactFraction>1 || !Double.isFinite(contactFraction))
            throw new IllegalArgumentException("Invalid experiment plan");
        var dates=new ArrayList<LocalDate>();dates.addAll(training);dates.addAll(validation);dates.addAll(test);
        for(int i=1;i<dates.size();i++) if(dates.get(i-1).plusDays(horizonDays).isAfter(dates.get(i)))
            throw new IllegalArgumentException("Observation groups must be ordered with nonoverlapping label windows");
    }
    public static ExperimentPlan standard() {
        return new ExperimentPlan(List.of(LocalDate.parse("2011-06-01"),LocalDate.parse("2011-07-01"),LocalDate.parse("2011-08-01")),
                List.of(LocalDate.parse("2011-09-01")),List.of(LocalDate.parse("2011-10-01"),LocalDate.parse("2011-11-01")),180,30,42,0.2);
    }
    public boolean labelAvailable(LocalDate observation, LocalDate coverageStart, LocalDate coverageEndExclusive) {
        return !observation.minusDays(historyDays).isBefore(coverageStart)
                && !observation.plusDays(horizonDays).isAfter(coverageEndExclusive);
    }
}
