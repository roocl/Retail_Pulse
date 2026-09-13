SELECT COUNT(*) AS fact_rows, COUNT(p.stock_code) AS product_matches,
       COUNT(c.customer_id) AS customer_matches
FROM transaction_facts f
LEFT JOIN dim_product p ON f.stock_code = p.stock_code
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
