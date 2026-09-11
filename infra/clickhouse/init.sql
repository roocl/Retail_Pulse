CREATE DATABASE IF NOT EXISTS retailpulse;

CREATE TABLE IF NOT EXISTS retailpulse.minute_metric_snapshots
(
    dataset String,
    result_version UInt64,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    paid_orders Int64,
    paid_users Int64,
    paid_quantity Int64,
    refunds Int64,
    gmv Decimal(38, 2),
    refund_amount Decimal(38, 2),
    received_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(result_version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (dataset, window_start)
TTL received_at + INTERVAL 90 DAY;

CREATE VIEW IF NOT EXISTS retailpulse.minute_metrics AS
SELECT dataset, result_version, window_start, window_end,
       paid_orders, paid_users, paid_quantity, refunds, gmv, refund_amount
FROM retailpulse.minute_metric_snapshots FINAL;

CREATE TABLE IF NOT EXISTS retailpulse.product_ranking_snapshots
(
    dataset String,
    result_version UInt64,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    products Array(Tuple(product_id String, gmv Decimal(38, 2), paid_orders Int64, paid_users Int64, paid_quantity Int64)),
    received_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(result_version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (dataset, window_start)
TTL received_at + INTERVAL 90 DAY;

CREATE VIEW IF NOT EXISTS retailpulse.product_rankings AS
SELECT dataset, result_version, window_start, window_end, products
FROM retailpulse.product_ranking_snapshots FINAL;

CREATE VIEW IF NOT EXISTS retailpulse.product_top_n AS
SELECT dataset, result_version, window_start, window_end, rank,
       product.product_id AS product_id, product.gmv AS gmv,
       product.paid_orders AS paid_orders, product.paid_users AS paid_users,
       product.paid_quantity AS paid_quantity
FROM retailpulse.product_rankings
ARRAY JOIN products AS product, arrayEnumerate(products) AS rank;
