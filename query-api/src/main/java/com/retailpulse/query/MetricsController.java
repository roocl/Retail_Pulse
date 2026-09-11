package com.retailpulse.query;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import java.time.Instant;
import java.time.Duration;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {
    private final MetricsRepository repository;

    public MetricsController(MetricsRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/latest")
    public ResponseEntity<MinuteMetric> latest(@RequestParam(defaultValue = "retailpulse") @NotBlank @Size(max = 128) String dataset) {
        return repository.latest(dataset).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/range")
    public MetricsRepository.MinuteRange range(
            @RequestParam(defaultValue = "retailpulse") @NotBlank @Size(max = 128) String dataset,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "120") @Min(1) @Max(1440) int limit) {
        validateTime(from);
        validateTime(to);
        if (!from.isBefore(to) || Duration.between(from, to).compareTo(Duration.ofDays(1)) > 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "时间范围必须递增且不超过 24 小时");
        }
        return repository.range(dataset, from, to, limit);
    }

    @GetMapping("/top-products")
    public MetricsRepository.Ranking ranking(
            @RequestParam(defaultValue = "retailpulse") @NotBlank @Size(max = 128) String dataset,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant windowStart,
            @RequestParam @Min(1) long resultVersion,
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int limit) {
        validateTime(windowStart);
        if (windowStart.getNano() != 0 || Math.floorMod(windowStart.getEpochSecond(), 60) != 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "窗口开始时间必须对齐到整分钟");
        }
        return repository.ranking(dataset, windowStart, resultVersion, limit);
    }

    private static void validateTime(Instant time) {
        if (time.isBefore(Instant.parse("1900-01-01T00:00:00Z"))
                || !time.isBefore(Instant.parse("2300-01-01T00:00:00Z")) || time.getNano() % 1000000 != 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "时间须在 1900–2299 年内，精度不超过毫秒");
        }
    }
}
