{{ config(
    materialized='table',
    schema='bronze',
    post_hook=[
        "
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'policies' AS dataset,
          'bronze' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.raw.policies) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_bronze.policies_bronze) tgt
        "
    ]
) }}

-- Bronze Policies: thin ingestion only
SELECT
    CAST(policy_id            AS STRING)        AS policy_id,
    CAST(customer_id          AS STRING)        AS customer_id,
    CAST(policy_type          AS STRING)        AS policy_type,
    CAST(coverage_amount      AS DECIMAL(18,2)) AS coverage_amount,
    CAST(premium_amount       AS DECIMAL(18,2)) AS premium_amount,
    CAST(deductible           AS DECIMAL(18,2)) AS deductible,
    CAST(start_date           AS DATE)          AS start_date,
    CAST(end_date             AS DATE)          AS end_date,
    CAST(status               AS STRING)        AS status,
    CAST(agent_id             AS STRING)        AS agent_id,
    CAST(underwriter_id       AS STRING)        AS underwriter_id,
    CAST(payment_frequency    AS STRING)        AS payment_frequency,
    CAST(created_at           AS TIMESTAMP)     AS created_at,
    CAST(updated_at           AS TIMESTAMP)     AS updated_at,
    CAST(_rescued_data        AS STRING)        AS _rescued_data,
    CAST(source_file_path     AS STRING)        AS source_file_path,
    CAST(source_file_time     AS TIMESTAMP)     AS source_file_time
FROM {{ source('raw', 'policies') }}
