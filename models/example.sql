-- name: my_first_model
-- materialized: table
-- partition_by: date

/*
    Welcome to your first dbx-sql-runner model!
    
    This is where you define your SQL logic.
    You can refer to other models like this: {upstream_model_name}
    Or refer to sources defined in profiles.yml like this: {my_source}
*/

SELECT 
    1 as id, 
    current_date() as date, 
    'Hello World' as message