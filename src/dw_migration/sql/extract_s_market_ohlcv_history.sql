SELECT
    asset_type,
    asset_code,
    market_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    partition_date,
    partition_hour,
    created_at
FROM {table}
WHERE partition_date = %s
ORDER BY asset_type, asset_code, market_time
