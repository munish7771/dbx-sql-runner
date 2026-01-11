-- name: fct_high_heat_alerts
-- materialized: table

SELECT 
    device_status,
    collect_list(device_id) as device_list,
    array_join(collect_list(device_id), ', ') as device_string,
    max(avg_temp_by_status) as max_group_temp
FROM {int_iot_agg}
GROUP BY device_status
HAVING max(avg_temp_by_status) > 30
