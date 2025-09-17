{{ config(
    materialized='table',
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'claims' AS dataset,
          'silver' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM {{ ref('claims_bronze') }}) src,
          (SELECT COUNT(*) AS target_count FROM {{ this }}) tgt
    "]
) }}

WITH cleaned AS (
    SELECT
        CAST(TRIM(claim_id) AS STRING) AS claim_id,
        CAST(TRIM(policy_id) AS STRING) AS policy_id,
        CAST(TRIM(customer_id) AS STRING) AS customer_id,
        CAST(claim_date AS TIMESTAMP) AS claim_date,
        CAST(reported_date AS TIMESTAMP) AS reported_date,
        CAST(claim_amount AS DOUBLE) AS claim_amount,
        CAST(settled_amount AS DOUBLE) AS settled_amount,
        CAST(deductible_amount AS INT) AS deductible_amount,
        INITCAP(TRIM(claim_reason)) AS claim_reason,
        UPPER(TRIM(status)) AS status,
        CAST(adjuster_id AS STRING) AS adjuster_id,
        UPPER(TRIM(claim_type)) AS claim_type,
        UPPER(TRIM(severity)) AS severity,
        CAST(fraud_indicator AS INT) AS fraud_indicator,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        source_file_path,
        source_file_time,
        CURRENT_TIMESTAMP() AS processed_at,
        -- Quality flags
        CASE WHEN claim_id IS NULL OR claim_id = '' THEN 1 ELSE 0 END AS missing_id_flag,
        CASE WHEN policy_id IS NULL OR policy_id = '' THEN 1 ELSE 0 END AS missing_policy_flag,
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END AS missing_customer_flag,
        CASE WHEN claim_amount IS NULL OR claim_amount < 0 THEN 1 ELSE 0 END AS invalid_claim_amount_flag,
        CASE WHEN settled_amount IS NULL OR settled_amount < 0 THEN 1 ELSE 0 END AS invalid_settled_amount_flag
    FROM {{ ref('claims_bronze') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY claim_id
            ORDER BY updated_at DESC, processed_at DESC
        ) AS row_num
    FROM cleaned
    WHERE missing_id_flag = 0
)

SELECT
    claim_id,
    policy_id,
    customer_id,
    claim_date,
    reported_date,
    claim_amount,
    settled_amount,
    deductible_amount,
    claim_reason,
    status,
    adjuster_id,
    claim_type,
    severity,
    fraud_indicator,
    created_at,
    updated_at,
    source_file_path,
    source_file_time,
    processed_at,
    -- Derived fields for analytics
    DATEDIFF(reported_date, claim_date) AS reporting_delay_days,
    claim_amount - settled_amount AS claim_difference,
    CASE WHEN claim_amount > 0 THEN ROUND(settled_amount / claim_amount, 3) ELSE NULL END AS settlement_ratio,
    -- Pass-through quality flags
    missing_policy_flag,
    missing_customer_flag,
    invalid_claim_amount_flag,
    invalid_settled_amount_flag
FROM deduped
WHERE row_num = 1


