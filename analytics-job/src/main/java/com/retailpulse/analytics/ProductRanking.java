package com.retailpulse.analytics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class ProductRanking {
    private static final Comparator<MinuteMetrics> ORDER = Comparator
            .comparing((MinuteMetrics value) -> value.gmv).reversed().thenComparing(value -> value.productId);
    public long windowStart;
    public long windowEnd;
    public List<MinuteMetrics> products;

    public ProductRanking() {}

    public static ProductRanking of(long start, long end, Iterable<MinuteMetrics> values, int limit) {
        if (limit < 1) throw new IllegalArgumentException("top-n must be positive");
        var best = new PriorityQueue<>(limit, ORDER.reversed());
        for (var value : values) {
            best.add(value);
            if (best.size() > limit) best.poll();
        }
        var result = new ProductRanking();
        result.windowStart = start;
        result.windowEnd = end;
        result.products = new ArrayList<>(best);
        result.products.sort(ORDER);
        return result;
    }
}
