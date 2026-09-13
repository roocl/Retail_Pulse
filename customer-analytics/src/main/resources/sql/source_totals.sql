SELECT COUNT(*) AS rows, COUNT(DISTINCT source_row_id) AS distinct_rows,
       CAST(SUM(TRY_CAST(quantity AS DECIMAL(18,6)) *
                TRY_CAST(unit_price AS DECIMAL(18,6))) AS DECIMAL(38,6)) AS amount
FROM raw_transactions
