{{ config(
    materialized='table',
     post_hook=[
        "
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'claims' AS dataset,
          'bronze' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.raw.claims) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_bronze.claims_bronze) tgt
        "
    ]
) }}

-- Bronze Claims: thin ingestion only (no dedupe, no heavy transformations)
SELECT
    CAST(claim_id AS STRING)              AS claim_id,
    CAST(policy_id AS STRING)             AS policy_id,
    CAST(customer_id AS STRING)           AS customer_id,
    CAST(claim_date AS TIMESTAMP)         AS claim_date,
    CAST(reported_date AS TIMESTAMP)      AS reported_date,
    CAST(claim_amount AS DOUBLE)          AS claim_amount,
    CAST(settled_amount AS DOUBLE)        AS settled_amount,
    CAST(deductible_amount AS INT)        AS deductible_amount,
    CAST(claim_reason AS STRING)          AS claim_reason,
    CAST(status AS STRING)                AS status,
    CAST(adjuster_id AS STRING)           AS adjuster_id,
    CAST(claim_type AS STRING)            AS claim_type,
    CAST(severity AS STRING)              AS severity,
    CAST(fraud_indicator AS INT)          AS fraud_indicator,
    CAST(created_at AS TIMESTAMP)         AS created_at,
    CAST(updated_at AS TIMESTAMP)         AS updated_at,

    -- metadata from ingestion
    CAST(source_file_path AS STRING)      AS source_file_path,
    CAST(source_file_time AS TIMESTAMP)   AS source_file_time
FROM {{ source('raw', 'claims') }}





