INSERT INTO {hist}
(
    asset_type, asset_code, market_time, open_price, high_price,
    low_price, close_price, volume, partition_date, partition_hour, created_at
)
SELECT
    src.asset_type,
    src.asset_code,
    src.market_time,
    src.open_price,
    src.high_price,
    src.low_price,
    src.close_price,
    src.volume,
    DATE(src.market_time) AS partition_date,
    HOUR(src.market_time) AS partition_hour,
    src.created_at
FROM {staging} AS src
ON DUPLICATE KEY UPDATE
    open_price = IF({unchanged}, {hist}.open_price, VALUES(open_price)),
    high_price = IF({unchanged}, {hist}.high_price, VALUES(high_price)),
    low_price = IF({unchanged}, {hist}.low_price, VALUES(low_price)),
    close_price = IF({unchanged}, {hist}.close_price, VALUES(close_price)),
    volume = IF({unchanged}, {hist}.volume, VALUES(volume)),
    partition_date = IF({unchanged}, {hist}.partition_date, VALUES(partition_date)),
    partition_hour = IF({unchanged}, {hist}.partition_hour, VALUES(partition_hour)),
    created_at = IF({unchanged}, {hist}.created_at, VALUES(created_at))
