SELECT COUNT(*) AS lines, COUNT(DISTINCT invoice_no) AS orders,
       COALESCE(SUM(signed_amount), CAST(0 AS DECIMAL(38,6))) AS gross_amount
FROM transaction_facts WHERE disposition = 'PURCHASE'
