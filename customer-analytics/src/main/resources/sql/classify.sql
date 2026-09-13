WITH parsed AS (
    SELECT source_row_id, CAST(source_row AS BIGINT) AS source_row, content_hash,
           NULLIF(TRIM(invoice_no), '') AS invoice_no,
           NULLIF(TRIM(stock_code), '') AS stock_code,
           NULLIF(TRIM(description), '') AS description,
           TRY_CAST(quantity AS DECIMAL(18, 6)) AS source_quantity,
           TRY_CAST(quantity AS BIGINT) AS quantity,
           TRY_CAST(invoice_time AS TIMESTAMP_NTZ) AS invoice_time,
           TRY_CAST(unit_price AS DECIMAL(18, 6)) AS unit_price,
           NULLIF(TRIM(customer_id), '') AS customer_id,
           NULLIF(TRIM(country), '') AS country,
           ROW_NUMBER() OVER (PARTITION BY content_hash ORDER BY CAST(source_row AS BIGINT)) AS occurrence,
           FIRST_VALUE(source_row_id) OVER (
               PARTITION BY content_hash ORDER BY CAST(source_row AS BIGINT)
           ) AS first_row_id
    FROM source_rows
), assessed AS (
    SELECT *,
           CAST(source_quantity * unit_price AS DECIMAL(30, 6)) AS signed_amount,
           CASE
               WHEN invoice_no IS NULL OR stock_code IS NULL THEN 'MISSING_TRANSACTION_KEY'
               WHEN invoice_time IS NULL THEN 'INVALID_TIME'
               WHEN quantity IS NULL OR source_quantity != quantity OR quantity = 0 THEN 'INVALID_QUANTITY'
               WHEN unit_price IS NULL OR unit_price <= 0 THEN 'NONPOSITIVE_OR_INVALID_PRICE'
               WHEN UPPER(invoice_no) LIKE 'C%' AND quantity > 0 THEN 'CANCELLATION_SIGN_CONFLICT'
               WHEN UPPER(invoice_no) NOT LIKE 'C%' AND quantity < 0 THEN 'NEGATIVE_WITHOUT_CANCELLATION'
               ELSE NULL
           END AS invalid_reason
    FROM parsed
)
SELECT source_row_id, source_row, content_hash, invoice_no, stock_code, description,
       quantity, CAST(invoice_time AS STRING) AS invoice_time,
       CAST(invoice_time AS DATE) AS invoice_date,
       unit_price, customer_id, country, signed_amount,
       COALESCE(DATE_FORMAT(invoice_time, 'yyyy-MM'), 'INVALID') AS transaction_month,
       CASE
           WHEN occurrence > 1 THEN 'EXCLUDED_DUPLICATE'
           WHEN invalid_reason IS NOT NULL THEN 'QUARANTINED'
           WHEN UPPER(invoice_no) LIKE 'C%' THEN 'CANCELLATION'
           ELSE 'PURCHASE'
       END AS disposition,
       CASE WHEN occurrence > 1 THEN 'SUSPECTED_EXACT_DUPLICATE' ELSE invalid_reason END AS reason,
       CASE WHEN occurrence > 1 THEN first_row_id ELSE NULL END AS duplicate_of,
       customer_id IS NOT NULL AND invalid_reason IS NULL AND occurrence = 1 AS customer_eligible,
       CASE WHEN UPPER(invoice_no) LIKE 'C%' THEN 'UNMATCHED' ELSE 'NOT_APPLICABLE' END AS cancellation_link
FROM assessed
