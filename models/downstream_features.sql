-- materialized: ddl
CREATE OR REPLACE TABLE downstream_features (
  id STRING COMMENT 'Unique store identifier',
  name STRING COMMENT 'Store name',
  opened_at TIMESTAMP COMMENT 'Store opening timestamp',
  opened_year INT GENERATED ALWAYS AS (YEAR(opened_at)) COMMENT 'Year store opened',
  opened_month INT GENERATED ALWAYS AS (MONTH(opened_at)) COMMENT 'Month store opened',
  opened_quarter INT GENERATED ALWAYS AS (QUARTER(opened_at)) COMMENT 'Quarter store opened',
  day_of_week STRING GENERATED ALWAYS AS (DATE_FORMAT(opened_at, "EEEE")) COMMENT 'Day of week store opened',
  is_weekend INT GENERATED ALWAYS AS (
    CASE WHEN DATE_FORMAT(opened_at, "u") IN ('6','7') THEN 1 ELSE 0 END
  ) COMMENT 'Is weekend (1/0)',
  tax_rate DOUBLE COMMENT 'Store tax rate',
  tax_category STRING COMMENT 'Tax category',
  new_feature DOUBLE COMMENT 'Experimental feature'
)
USING DELTA
PARTITIONED BY (opened_year, opened_month)
TBLPROPERTIES (
  'description' = 'Feature table for stores with generated columns and metadata',
  'created_by' = 'Databricks SQL DDL example'
);