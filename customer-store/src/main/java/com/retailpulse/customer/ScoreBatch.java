package com.retailpulse.customer;

import java.util.List;

public record ScoreBatch(String id,String profileBatchId,String modelId,String algorithm,int horizonDays) {
    public ScoreBatch {
        for(String field:List.of(id,profileBatchId,modelId,algorithm))
            if(field.isBlank() || field.length()>64)throw new IllegalArgumentException("Invalid score batch identity");
        if(horizonDays<1)throw new IllegalArgumentException("Invalid prediction horizon");
    }
}
