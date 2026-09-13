SELECT COUNT(*) AS rows, COUNT(DISTINCT source_row_id) AS distinct_rows,
       CAST(SUM(signed_amount) AS DECIMAL(38,6)) AS amount
FROM transaction_audit
