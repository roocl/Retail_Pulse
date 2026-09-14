package com.retailpulse.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class QueryErrors {
    private static final Logger LOG = LoggerFactory.getLogger(QueryErrors.class);

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail invalid(IllegalArgumentException error) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "查询参数或批次无效");
    }

    @ExceptionHandler(DataAccessException.class)
    ProblemDetail unavailable(DataAccessException error) {
        LOG.error("Storage query failed", error);
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "数据存储暂时不可用，请稍后重试");
    }
}
