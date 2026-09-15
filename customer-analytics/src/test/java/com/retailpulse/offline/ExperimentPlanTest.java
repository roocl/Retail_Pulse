package com.retailpulse.offline;

import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ExperimentPlanTest {
    @Test void labelsMustFinishBeforeNextSplitAndDatasetEnd() {
        var plan=ExperimentPlan.standard();
        assertTrue(plan.labelAvailable(LocalDate.parse("2011-11-01"),LocalDate.parse("2010-12-01"),LocalDate.parse("2011-12-09")));
        assertFalse(plan.labelAvailable(LocalDate.parse("2011-12-01"),LocalDate.parse("2010-12-01"),LocalDate.parse("2011-12-09")));
        assertFalse(plan.labelAvailable(LocalDate.parse("2011-01-01"),LocalDate.parse("2010-12-01"),LocalDate.parse("2011-12-09")));
        assertThrows(IllegalArgumentException.class,()->new ExperimentPlan(
                List.of(LocalDate.parse("2011-08-15")),List.of(LocalDate.parse("2011-09-01")),
                List.of(LocalDate.parse("2011-10-01")),180,30,42,0.2));
    }
}
