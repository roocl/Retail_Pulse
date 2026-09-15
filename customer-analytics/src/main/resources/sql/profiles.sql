WITH thresholds AS (
    SELECT PERCENTILE(recency_days, ARRAY(0.3333333333,0.6666666667)) AS r,
           PERCENTILE(orders, ARRAY(0.3333333333,0.6666666667)) AS f,
           PERCENTILE(purchase_amount, ARRAY(0.3333333333,0.6666666667)) AS m
    FROM customer_features
), scores AS (
    SELECT t.*,
           CASE WHEN recency_days <= r[0] THEN 3 WHEN recency_days <= r[1] THEN 2 ELSE 1 END AS r_score,
           CASE WHEN orders <= f[0] THEN 1 WHEN orders <= f[1] THEN 2 ELSE 3 END AS f_score,
           CASE WHEN purchase_amount <= m[0] THEN 1 WHEN purchase_amount <= m[1] THEN 2 ELSE 3 END AS m_score
    FROM customer_features t CROSS JOIN thresholds
)
SELECT *, CASE WHEN orders=1 THEN 'SINGLE_PURCHASE'
               WHEN r_score=1 THEN 'AT_RISK'
               WHEN f_score=3 AND m_score=3 THEN 'HIGH_VALUE'
               ELSE 'REPEAT_CUSTOMER' END AS segment
FROM scores
