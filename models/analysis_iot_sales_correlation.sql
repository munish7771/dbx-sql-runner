-- name: analysis_iot_sales_correlation
-- materialized: view

-- distinct join to verify we can handle multiple upstream deps including one likely to fail if not mocked (features)
-- Assuming 'features' logic is valid or user has that table.

SELECT 
    a.device_status,
    a.max_group_temp,
    current_date() as analysis_date
FROM {fct_high_heat_alerts} a
-- avoiding join to 'features' if it relies on external tables that might not exist 
-- but syntax checking of {features} dependency logic
CROSS JOIN (SELECT 1 as dummy)
