package com.retailpulse.analytics;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

class CommerceMetricsTest {
    @Test
    void ranksByGrossAmountThenProductIdRegardlessOfArrivalOrder() {
        var a = new CommerceTotals();
        a.add(EventParserTest.parse(EventParserTest.VALID));
        var b = new CommerceTotals();
        b.add(EventParserTest.parse(EventParserTest.VALID));
        var c = new CommerceTotals();
        c.add(EventParserTest.parse(EventParserTest.VALID.replace("12.30", "20.00")));
        var input = new java.util.ArrayList<>(java.util.List.of(
                b.finish(0, 60000, "b"), c.finish(0, 60000, "c"), a.finish(0, 60000, "a")));
        for (int i = 0; i < 10; i++) {
            java.util.Collections.shuffle(input, new java.util.Random(i));
            var ranking = ProductRanking.of(0, 60000, input, 2);
            assertEquals(java.util.List.of("c", "a"), ranking.products.stream().map(p -> p.productId).toList());
        }
        assertEquals(3, ProductRanking.of(0, 60000, input, 10).products.size());
        assertTrue(ProductRanking.of(0, 60000, java.util.List.of(), 10).products.isEmpty());
    }

    @Test
    void countsOrdersAndUsersAndKeepsRefundsSeparateFromGrossPayments() {
        var totals = new CommerceTotals();
        totals.add(EventParserTest.parse(EventParserTest.VALID));
        totals.add(EventParserTest.parse(EventParserTest.VALID.replace("e1", "e2")));
        totals.add(EventParserTest.parse(EventParserTest.VALID.replace("e1", "e3").replace("o1", "o2").replace("12.30", "0.20")));
        totals.add(EventParserTest.parse(EventParserTest.VALID.replace("e1", "e4").replace("o1", "o3").replace("u1", "u2").replace("12.30", "0.10")));
        totals.add(EventParserTest.parse(EventParserTest.VALID.replace("e1", "r1").replace("PAYMENT_COMPLETED", "REFUND_COMPLETED").replace("12.30", "2.30")));
        var result = totals.finish(0, 60000, null);
        assertEquals(3, result.paidOrders);
        assertEquals(2, result.paidUsers);
        assertEquals(3, result.paidQuantity);
        assertEquals(new BigDecimal("12.60"), result.gmv);
        assertEquals(1, result.refunds);
        assertEquals(new BigDecimal("2.30"), result.refundAmount);
    }
}
