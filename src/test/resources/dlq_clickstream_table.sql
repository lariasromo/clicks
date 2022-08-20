CREATE TABLE dlq_clickstream
(
    `topic` String,
    `value` String,
    `createdDate` DateTime
)
    ENGINE = MergeTree()
PARTITION BY toYYYYMM("createdDate")
ORDER BY toYYYYMM("createdDate");