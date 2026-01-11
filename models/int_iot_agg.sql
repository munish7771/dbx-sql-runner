-- name: int_iot_agg
-- materialized: table
-- partition_by: device_status

SELECT 
    parsed.device_id,
    parsed.status as device_status,
    avg(parsed.temp) over (partition by parsed.status) as avg_temp_by_status,
    parsed.temp - avg(parsed.temp) over (partition by parsed.status) as temp_diff,
    load_time
FROM {stg_iot_devices}
WHERE parsed.temp IS NOT NULL
