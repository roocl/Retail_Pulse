package com.retailpulse.analytics;

import com.retailpulse.common.EventType;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Map;

public final class MetricsPipeline {
    private static final TypeInformation<CommerceTotals> TOTALS = Types.POJO(CommerceTotals.class, Map.of(
            "orders", Types.MAP(Types.STRING, Types.BOOLEAN), "users", Types.MAP(Types.STRING, Types.BOOLEAN),
            "refunds", Types.LONG, "paidQuantity", Types.LONG, "gmv", Types.BIG_DEC, "refundAmount", Types.BIG_DEC));
    private static final TypeInformation<MinuteMetrics> MINUTE = Types.POJO(MinuteMetrics.class);
    public static final OutputTag<MinuteMetrics> LATE_PRODUCTS = new OutputTag<>("late-products", MINUTE);
    private static final TypeInformation<ProductRanking> RANKING = Types.POJO(ProductRanking.class, Map.of(
            "windowStart", Types.LONG, "windowEnd", Types.LONG, "products", Types.LIST(MINUTE)));
    private MetricsPipeline() {}

    public static Streams attach(DataStream<IngestedRecord> events, int topN) {
        var minutes = events.filter(e -> e.eventType == EventType.PAYMENT_COMPLETED || e.eventType == EventType.REFUND_COMPLETED)
                .name("transactions").uid("transactions-v2")
                .keyBy(e -> "all").window(TumblingEventTimeWindows.of(MinuteWindow.SIZE))
                .sideOutputLateData(DeduplicateEvents.LATE_EVENTS)
                .aggregate(new Aggregate(), new Finish(false), TOTALS, TOTALS, MINUTE).name("minute-metrics").uid("minute-metrics-v2");
        var products = events.filter(e -> e.eventType == EventType.PAYMENT_COMPLETED)
                .name("payments").uid("payments-v2")
                .keyBy(e -> e.productId).window(TumblingEventTimeWindows.of(MinuteWindow.SIZE))
                .sideOutputLateData(DeduplicateEvents.LATE_EVENTS)
                .aggregate(new Aggregate(), new Finish(true), TOTALS, TOTALS, MINUTE).name("product-metrics").uid("product-metrics-v2");
        var ranking = products.keyBy(p -> p.windowEnd).window(TumblingEventTimeWindows.of(MinuteWindow.SIZE))
                .sideOutputLateData(LATE_PRODUCTS)
                .process(new Rank(topN), RANKING).name("product-top-n").uid("product-top-n-v2");
        return new Streams(minutes, products, ranking);
    }

    public record Streams(SingleOutputStreamOperator<MinuteMetrics> minutes,
                          SingleOutputStreamOperator<MinuteMetrics> products,
                          SingleOutputStreamOperator<ProductRanking> ranking) {}

    public static class Aggregate implements AggregateFunction<IngestedRecord, CommerceTotals, CommerceTotals> {
        @Override
        public CommerceTotals createAccumulator() { return new CommerceTotals(); }

        @Override
        public CommerceTotals add(IngestedRecord value, CommerceTotals accumulator) {
            accumulator.add(value);
            return accumulator;
        }

        @Override
        public CommerceTotals getResult(CommerceTotals accumulator) { return accumulator; }

        @Override
        public CommerceTotals merge(CommerceTotals left, CommerceTotals right) {
            throw new UnsupportedOperationException("Only non-merging tumbling windows are supported");
        }
    }

    public static class Finish extends ProcessWindowFunction<CommerceTotals, MinuteMetrics, String, TimeWindow> {
        private final boolean product;

        public Finish(boolean product) { this.product = product; }

        @Override
        public void process(String key, Context context, Iterable<CommerceTotals> values, Collector<MinuteMetrics> out) {
            out.collect(values.iterator().next().finish(context.window().getStart(), context.window().getEnd(), product ? key : null));
        }
    }

    public static class Rank extends ProcessWindowFunction<MinuteMetrics, ProductRanking, Long, TimeWindow> {
        private final int limit;

        public Rank(int limit) { this.limit = limit; }

        @Override
        public void process(Long key, Context context, Iterable<MinuteMetrics> values, Collector<ProductRanking> out) {
            out.collect(ProductRanking.of(context.window().getStart(), context.window().getEnd(), values, limit));
        }
    }
}
