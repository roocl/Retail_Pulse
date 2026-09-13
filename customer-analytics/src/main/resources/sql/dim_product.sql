SELECT stock_code, description AS latest_observed_description
FROM (
    SELECT stock_code, description,
           ROW_NUMBER() OVER (
               PARTITION BY stock_code
               ORDER BY CASE WHEN description IS NULL THEN 1 ELSE 0 END,
                        invoice_time DESC, source_row DESC
           ) AS position
    FROM transaction_facts
) WHERE position = 1
