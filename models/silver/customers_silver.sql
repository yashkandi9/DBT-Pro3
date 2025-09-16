{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'customers'],
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'customers' AS dataset,
          'silver' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_bronze.customers_bronze) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_silver.customers_silver) tgt
    "]
) }}

WITH cleaned AS (
    SELECT
        CAST(TRIM(customer_id) AS STRING) AS customer_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,
        CAST(date_of_birth AS DATE) AS date_of_birth,
        TRIM(address) AS address,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(zip_code) AS zip_code,
        CAST(annual_income AS DECIMAL(18,2)) AS annual_income,
        CAST(credit_score AS INT) AS credit_score,
        INITCAP(TRIM(marital_status)) AS marital_status,
        INITCAP(TRIM(occupation)) AS occupation,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        source_file_path,
        source_file_time,
        -- Add audit fields
        CURRENT_TIMESTAMP() AS processed_at,
        -- Business derivations
        CASE 
            WHEN date_of_birth IS NOT NULL THEN YEAR(CURRENT_DATE()) - YEAR(date_of_birth)
            ELSE NULL END AS age,
        CONCAT(INITCAP(TRIM(first_name)), ' ', INITCAP(TRIM(last_name))) AS full_name,
        -- Data quality flags
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END AS missing_id_flag,
        CASE WHEN email IS NULL 
              OR email NOT RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
              THEN 1
              ELSE 0 END AS invalid_email_flag
    FROM {{ ref('customers_bronze') }}
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY updated_at DESC, processed_at DESC
           ) AS row_num
    FROM cleaned
    WHERE missing_id_flag = 0
)

SELECT
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    date_of_birth,
    age,
    address,
    city,
    state,
    zip_code,
    annual_income,
    credit_score,
    marital_status,
    occupation,
    created_at,
    updated_at,
    source_file_path,
    source_file_time,
    processed_at,
    invalid_email_flag
FROM deduped
WHERE row_num = 1
