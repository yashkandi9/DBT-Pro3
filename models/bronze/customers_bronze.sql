{{ config(
    materialized='table',
    schema='bronze',
    post_hook=[
        "
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'customers' AS dataset,
          'bronze' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.raw.customers) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_bronze.customers_bronze) tgt
        "
    ]
) }}

-- Bronze Customers: thin ingestion only
SELECT
    CAST(customer_id AS STRING)           AS customer_id,
    CAST(first_name AS STRING)            AS first_name,
    CAST(last_name AS STRING)             AS last_name,
    CAST(email AS STRING)                 AS email,
    CAST(phone AS STRING)                 AS phone,
    CAST(date_of_birth AS DATE)           AS date_of_birth,
    CAST(address AS STRING)               AS address,
    CAST(city AS STRING)                  AS city,
    CAST(state AS STRING)                 AS state,
    CAST(zip_code AS STRING)              AS zip_code,
    CAST(annual_income AS DECIMAL(18,2))  AS annual_income,
    CAST(credit_score AS INT)             AS credit_score,
    CAST(marital_status AS STRING)        AS marital_status,
    CAST(occupation AS STRING)            AS occupation,
    CAST(created_at AS TIMESTAMP)         AS created_at,
    CAST(updated_at AS TIMESTAMP)         AS updated_at,
    CAST(source_file_path AS STRING)      AS source_file_path,
    CAST(source_file_time AS TIMESTAMP)   AS source_file_time
FROM {{ source('raw', 'customers') }}
