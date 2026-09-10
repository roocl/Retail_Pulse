package com.retailpulse.analytics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class MetricsWindowsTest {
    private static <K, I, O> KeyedOneInputStreamOperatorTestHarness<K, I, O> harness(
            SingleOutputStreamOperator<O> output, KeySelector<I, K> selector, TypeInformation<K> keyType) throws Exception {
        var transformation = (OneInputTransformation<I, O>) output.getTransformation();
        var factory = (SimpleOperatorFactory<O>) transformation.getOperatorFactory();
        var operator = (OneInputStreamOperator<I, O>) factory.getOperator();
        var test = new KeyedOneInputStreamOperatorTestHarness<>(operator, selector, keyType);
        test.getExecutionConfig().disableGenericTypes();
        test.setup(output.getType().createSerializer(test.getExecutionConfig()));
        return test;
    }

    @Test
    void closesUtcMinutesOnceAndFeedsProductResultsIntoTheSameRankingWindow() throws Exception {
        try (var env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.getConfig().disableGenericTypes();
            var record = EventParserTest.parse(EventParserTest.VALID);
            var streams = MetricsPipeline.attach(env.fromData(record), 2);
            try (var global = harness(streams.minutes(), (IngestedRecord e) -> "all", Types.STRING);
                 var products = harness(streams.products(), (IngestedRecord e) -> e.productId, Types.STRING);
                 var ranking = harness(streams.ranking(), (MinuteMetrics m) -> m.windowEnd, Types.LONG)) {
                global.open();
                products.open();
                ranking.open();
                var early = EventParserTest.parse(EventParserTest.VALID.replace("e1", "e2").replace("o1", "o2").replace("p1", "p2").replace("12.30", "20.00"));
                record.eventTime += 59_999;
                var next = EventParserTest.parse(EventParserTest.VALID.replace("e1", "e3").replace("o1", "o3"));
                next.eventTime += 60_000;
                for (var e : java.util.List.of(record, next, early)) {
                    global.processElement(new StreamRecord<>(e, e.eventTime));
                    products.processElement(new StreamRecord<>(e, e.eventTime));
                }
                long close = MinuteWindow.end(early.eventTime) - 1;
                global.processWatermark(new Watermark(close - 1));
                assertTrue(global.extractOutputStreamRecords().isEmpty());
                global.processWatermark(new Watermark(close));
                products.processWatermark(new Watermark(close));
                var minute = global.extractOutputStreamRecords().get(0).getValue();
                assertEquals(new BigDecimal("32.30"), minute.gmv);
                assertEquals(2, minute.paidOrders);
                assertEquals(1, minute.paidUsers);
                assertEquals(early.eventTime, minute.windowStart);
                for (var product : products.extractOutputStreamRecords()) {
                    assertEquals(close, product.getTimestamp());
                    ranking.processElement(new StreamRecord<>(product.getValue(), product.getTimestamp()));
                }
                ranking.processWatermark(new Watermark(close));
                var top = ranking.extractOutputStreamRecords().get(0).getValue();
                assertEquals(minute.windowEnd, top.windowEnd);
                assertEquals(java.util.List.of("p2", "p1"), top.products.stream().map(p -> p.productId).toList());
                global.processElement(new StreamRecord<>(early, early.eventTime));
                products.processElement(new StreamRecord<>(early, early.eventTime));
                assertEquals(1, global.getSideOutput(DeduplicateEvents.LATE_EVENTS).size());
                assertEquals(1, products.getSideOutput(DeduplicateEvents.LATE_EVENTS).size());
                ranking.processElement(new StreamRecord<>(top.products.get(0), close));
                assertEquals(1, ranking.getSideOutput(MetricsPipeline.LATE_PRODUCTS).size());
                global.processWatermark(new Watermark(close + 60_000));
                assertEquals(2, global.extractOutputStreamRecords().size());
                assertEquals(new BigDecimal("12.30"), global.extractOutputStreamRecords().get(1).getValue().gmv);
            }
        }
    }
}
