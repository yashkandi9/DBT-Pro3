{{ config(
    materialized='table',
    schema='silver',
    post_hook=[
        """
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'policies' AS dataset,
          'silver' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_bronze.policies_bronze) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_silver.policies_silver) tgt
        """
    ]
) }}

WITH cleaned AS (
    SELECT
        CAST(TRIM(policy_id) AS STRING) AS policy_id,
        CAST(TRIM(customer_id) AS STRING) AS customer_id,
        INITCAP(TRIM(policy_type)) AS policy_type,
        CAST(coverage_amount AS DECIMAL(18,2)) AS coverage_amount,
        CAST(premium_amount AS DECIMAL(18,2)) AS premium_amount,
        CAST(deductible AS DECIMAL(18,2)) AS deductible,
        CAST(start_date AS DATE) AS start_date,
        CAST(end_date AS DATE) AS end_date,
        UPPER(TRIM(status)) AS status,
        CAST(agent_id AS STRING) AS agent_id,
        CAST(underwriter_id AS STRING) AS underwriter_id,
        LOWER(TRIM(payment_frequency)) AS payment_frequency,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        source_file_path,
        source_file_time,
        CURRENT_TIMESTAMP() AS processed_at,
        -- Data Quality Flags
        CASE WHEN policy_id IS NULL OR policy_id = '' THEN 1 ELSE 0 END AS missing_policy_id_flag,
        CASE WHEN customer_id IS NULL OR customer_id = '' THEN 1 ELSE 0 END AS missing_customer_id_flag,
        CASE WHEN coverage_amount IS NULL OR coverage_amount < 0 THEN 1 ELSE 0 END AS invalid_coverage_amount_flag,
        CASE WHEN premium_amount IS NULL OR premium_amount < 0 THEN 1 ELSE 0 END AS invalid_premium_amount_flag,
        CASE WHEN deductible IS NULL OR deductible < 0 THEN 1 ELSE 0 END AS invalid_deductible_flag,
        CASE 
            WHEN start_date IS NULL OR end_date IS NULL OR start_date > end_date THEN 1 
            ELSE 0 END AS invalid_date_range_flag,
        -- Business Enrichment
        DATEDIFF(end_date, start_date) AS policy_duration_days
    FROM {{ ref('policies_bronze') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY policy_id
            ORDER BY updated_at DESC, processed_at DESC
        ) AS row_num
    FROM cleaned
    WHERE missing_policy_id_flag = 0
)

SELECT
    policy_id,
    customer_id,
    policy_type,
    coverage_amount,
    premium_amount,
    deductible,
    start_date,
    end_date,
    status,
    agent_id,
    underwriter_id,
    payment_frequency,
    created_at,
    updated_at,
    source_file_path,
    source_file_time,
    processed_at,
    policy_duration_days,
    -- Data Quality Flags for downstream use
    missing_customer_id_flag,
    invalid_coverage_amount_flag,
    invalid_premium_amount_flag,
    invalid_deductible_flag,
    invalid_date_range_flag
FROM deduped
WHERE row_num = 1
