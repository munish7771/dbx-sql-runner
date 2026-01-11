-- name: stg_iot_devices
-- materialized: view

WITH raw_json AS (
    SELECT 
        1 as id, 
        '{"device_id": "D1", "temp": 25.5, "status": "active"}' as payload, 
        current_timestamp() as load_time
    UNION ALL
    SELECT 
        2 as id, 
        '{"device_id": "D2", "temp": 80.1, "status": "error"}' as payload,
        current_timestamp() as load_time
)
SELECT 
    id,
    from_json(payload, 'device_id STRING, temp DOUBLE, status STRING') as parsed,
    load_time
FROM raw_json
