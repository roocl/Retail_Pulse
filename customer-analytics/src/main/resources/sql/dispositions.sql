SELECT disposition, reason, COUNT(*) AS rows,
       SUM(signed_amount) AS signed_amount,
       COUNT_IF(signed_amount IS NULL) AS unquantified_amount_rows,
       COUNT_IF(customer_id IS NULL) AS missing_customer_rows
FROM transaction_audit GROUP BY disposition, reason ORDER BY disposition, reason
