WITH profile_facts AS (
    SELECT * FROM profile_source WHERE customer_id IS NOT NULL
    AND invoice_time >= '${window_start}' AND invoice_time < '${observation}'
), purchases AS (
    SELECT * FROM profile_facts WHERE disposition='PURCHASE'
), totals AS (
    SELECT customer_id, COUNT(DISTINCT invoice_no) AS orders,
           CAST(SUM(signed_amount) AS DECIMAL(30,6)) AS purchase_amount,
           DATEDIFF(DATE '${observation}', MAX(CAST(invoice_time AS DATE))) AS recency_days,
           MIN(invoice_time) AS first_purchase, MAX(invoice_time) AS last_purchase
    FROM purchases GROUP BY customer_id
), cancellations AS (
    SELECT customer_id, CAST(SUM(signed_amount) AS DECIMAL(30,6)) AS cancellation_amount
    FROM profile_facts WHERE disposition='CANCELLATION' GROUP BY customer_id
), countries AS (
    SELECT customer_id, country, ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY invoice_time DESC, source_row_id DESC) AS position
    FROM profile_facts
), products AS (
    SELECT customer_id, stock_code, SUM(signed_amount) AS amount
    FROM purchases GROUP BY customer_id, stock_code
), preferred AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, stock_code) AS position
    FROM products
)
SELECT t.*, COALESCE(c.cancellation_amount, CAST(0 AS DECIMAL(30,6))) AS cancellation_amount,
       country, stock_code AS preferred_product
FROM totals t
LEFT JOIN cancellations c ON t.customer_id=c.customer_id
LEFT JOIN countries n ON t.customer_id=n.customer_id AND n.position=1
LEFT JOIN preferred p ON t.customer_id=p.customer_id AND p.position=1
