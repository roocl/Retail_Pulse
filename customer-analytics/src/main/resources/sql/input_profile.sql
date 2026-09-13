SELECT MIN(invoice_time) AS first_local_time, MAX(invoice_time) AS last_local_time,
       COUNT_IF(customer_id IS NULL) AS missing_customer_rows,
       COUNT_IF(description IS NULL) AS missing_description_rows,
       COUNT_IF(quantity < 0) AS negative_quantity_rows,
       COUNT_IF(unit_price <= 0) AS nonpositive_price_rows,
       COUNT_IF(UPPER(invoice_no) LIKE 'C%') AS cancellation_rows,
       COUNT(DISTINCT invoice_no) AS invoices,
       COUNT(DISTINCT stock_code) AS products,
       COUNT(DISTINCT customer_id) AS identified_customers
FROM transaction_audit
