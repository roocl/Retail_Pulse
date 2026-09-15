WITH future_buyers AS (
    SELECT DISTINCT customer_id FROM label_source
    WHERE disposition='PURCHASE' AND customer_id IS NOT NULL
      AND invoice_time >= '${observation}' AND invoice_time < '${label_end}'
)
SELECT f.*, '${observation}' AS observation,
       CAST(CASE WHEN b.customer_id IS NULL THEN 0 ELSE 1 END AS DOUBLE) AS label
FROM historical_features f LEFT JOIN future_buyers b ON f.customer_id=b.customer_id
