{{ config( 
    materialized='table',
    schema='silver',
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'premiums' AS dataset,
          'silver' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_bronze.premiums_bronze) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_silver.premiums_silver) tgt
    "]
) }}

WITH source AS (
    SELECT * 
    FROM {{ ref('premiums_bronze') }}
),

deduped AS (
    SELECT
        CAST(TRIM(premium_id) AS STRING)        AS premium_id,
        CAST(TRIM(policy_id) AS STRING)         AS policy_id,
        CAST(TRIM(customer_id) AS STRING)       AS customer_id,
        CAST(payment_date AS TIMESTAMP)         AS payment_date,
        CAST(due_date AS TIMESTAMP)             AS due_date,
        CAST(premium_amount AS DOUBLE)          AS premium_amount,
        CAST(TRIM(payment_frequency) AS STRING) AS payment_frequency,
        CAST(TRIM(payment_method) AS STRING)    AS payment_method,
        CAST(TRIM(payment_status) AS STRING)    AS payment_status,
        CAST(late_fee AS DOUBLE)                AS late_fee,
        CAST(discount_applied AS DOUBLE)        AS discount_applied,
        CAST(tax_amount AS DOUBLE)              AS tax_amount,
        CAST(total_amount AS DOUBLE)            AS total_amount,
        CAST(TRIM(transaction_id) AS STRING)    AS transaction_id,
        CAST(TRIM(payment_processor) AS STRING) AS payment_processor,
        CAST(created_at AS TIMESTAMP)           AS created_at,
        CAST(updated_at AS TIMESTAMP)           AS updated_at,
        CAST(source_file_path AS STRING)        AS source_file_path,
        CAST(source_file_time AS TIMESTAMP)     AS source_file_time,
        ROW_NUMBER() OVER (
            PARTITION BY premium_id
            ORDER BY source_file_time DESC
        ) AS rn
    FROM source
),

cleaned AS (
    SELECT
        premium_id,
        policy_id,
        customer_id,
        payment_date,
        due_date,
        CASE WHEN premium_amount < 0 THEN 0 ELSE premium_amount END AS premium_amount,
        UPPER(TRIM(payment_frequency)) AS payment_frequency,
        INITCAP(TRIM(payment_method))  AS payment_method,
        UPPER(TRIM(payment_status))    AS payment_status,
        CASE WHEN late_fee < 0 THEN 0 ELSE late_fee END                 AS late_fee,
        CASE WHEN discount_applied < 0 THEN 0 ELSE discount_applied END AS discount_applied,
        CASE WHEN tax_amount < 0 THEN 0 ELSE tax_amount END             AS tax_amount,
        CASE WHEN total_amount < 0 THEN 0 ELSE total_amount END         AS total_amount,
        transaction_id,
        INITCAP(TRIM(payment_processor)) AS payment_processor,
        created_at,
        updated_at,
        source_file_path,
        source_file_time
    FROM deduped
    WHERE rn = 1
)

SELECT * FROM cleaned
