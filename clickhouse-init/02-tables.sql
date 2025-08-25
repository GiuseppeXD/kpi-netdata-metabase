CREATE TABLE IF NOT EXISTS netdata_metrics.metrics (
    timestamp DateTime64(3) CODEC(Delta, ZSTD(1)),
    hostname LowCardinality(String),
    chart_id LowCardinality(String),
    chart_name String,
    dimension LowCardinality(String),
    value Float64 CODEC(Gorilla, ZSTD(1)),
    units LowCardinality(String),
    family LowCardinality(String),
    context LowCardinality(String),
    chart_type LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (hostname, chart_id, dimension, timestamp)
TTL toDateTime(timestamp) + toIntervalDay(30)
SETTINGS index_granularity = 8192;