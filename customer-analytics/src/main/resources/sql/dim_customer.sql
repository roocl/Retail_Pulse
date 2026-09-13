SELECT customer_id, country AS latest_observed_country
FROM (
    SELECT customer_id, country,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id ORDER BY invoice_time DESC, source_row DESC
           ) AS position
    FROM transaction_facts WHERE customer_id IS NOT NULL
) WHERE position = 1
