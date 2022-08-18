SET allow_experimental_object_type = 1;
CREATE TABLE alexandria.clickstream
(
    `clientId` String null,
    `eventType` String null,
    `country` String null,
    `broker` String null,
    `sessionId` String null,
    `visitorId` String null,
    `gpsAdid` String null,
    `idfa` String null,
    `trackerType` String,
    `createdAt` DateTime32,
    `origin` String,
    `uriParamsJson` JSON,
    insertedAt timestamp
)
    ENGINE = MergeTree()
ORDER BY (origin, "trackerType", "sessionId", "visitorId", "gpsAdid", idfa, broker)
PARTITION BY toYYYYMM("createdAt")
SETTINGS allow_nullable_key = 1;

-- ideally add TTL
-- TTL "createdAt" + INTERVAL 2 WEEK TO DISK 's3
-- This feature is under development and not ready for production;
