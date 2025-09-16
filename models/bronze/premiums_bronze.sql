{{ config(
    materialized='table',
    schema='bronze',
    post_hook=[
        "
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'premiums' AS dataset,
          'bronze' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.raw.premiums) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_bronze.premiums_bronze) tgt
        "
    ]
) }}

-- Bronze Premiums: thin ingestion only
select
    cast(trim(premium_id) as string) as premium_id,
    cast(trim(policy_id) as string) as policy_id,
    cast(trim(customer_id) as string) as customer_id,
    cast(payment_date as timestamp) as payment_date,
    cast(due_date as timestamp) as due_date,
    cast(premium_amount as double) as premium_amount,
    cast(trim(payment_frequency) as string) as payment_frequency,
    cast(trim(payment_method) as string) as payment_method,
    cast(trim(payment_status) as string) as payment_status,
    cast(late_fee as double) as late_fee,
    cast(discount_applied as double) as discount_applied,
    cast(tax_amount as double) as tax_amount,
    cast(total_amount as double) as total_amount,
    cast(trim(transaction_id) as string) as transaction_id,
    cast(trim(payment_processor) as string) as payment_processor,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,

    -- Persisted metadata
    cast(source_file_path as string) as source_file_path,
    cast(source_file_time as timestamp) as source_file_time
from {{ source("raw", "premiums") }}
