{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'claims_operations'],
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'claims_operations' AS dataset,
          'gold' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_silver.claims_silver) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_gold.gold_claims_operations) tgt
    "]
) }}

WITH claims_base AS (
    SELECT 
        cl.claim_type,
        cl.severity,
        cl.status,
        cl.adjuster_id,
        DATE_TRUNC('month', cl.claim_date) as claim_month,
        p.policy_type,
        c.state as customer_state,
        c.age as customer_age,
        COUNT(*) as claim_count,
        SUM(cl.claim_amount) as total_claim_amount,
        SUM(cl.settled_amount) as total_settled_amount,
        AVG(cl.claim_amount) as avg_claim_amount,
        AVG(cl.settled_amount) as avg_settled_amount,
        AVG(cl.reporting_delay_days) as avg_reporting_delay,
        AVG(cl.settlement_ratio) as avg_settlement_ratio,
        COUNT(CASE WHEN cl.fraud_indicator = 1 THEN 1 END) as potential_fraud_cases
    FROM {{ ref('claims_silver') }} cl
    JOIN {{ ref('policies_silver') }} p ON cl.policy_id = p.policy_id
    JOIN {{ ref('customers_silver') }} c ON p.customer_id = c.customer_id
    WHERE cl.missing_policy_flag = 0
    GROUP BY cl.claim_type, cl.severity, cl.status, cl.adjuster_id, 
             DATE_TRUNC('month', cl.claim_date), p.policy_type, c.state, c.age
),

adjuster_performance AS (
    SELECT 
        adjuster_id,
        COUNT(*) as total_claims_handled,
        AVG(settlement_ratio) as avg_settlement_ratio,
        AVG(reporting_delay_days) as avg_processing_time,
        SUM(CASE WHEN fraud_indicator = 1 THEN 1 ELSE 0 END) as fraud_cases_detected
    FROM {{ ref('claims_silver') }}
    WHERE adjuster_id IS NOT NULL
    GROUP BY adjuster_id
)

SELECT 
    cb.claim_type,
    cb.severity,
    cb.status,
    cb.adjuster_id,
    cb.claim_month,
    cb.policy_type,
    cb.customer_state,
    
    -- Volume Metrics
    cb.claim_count,
    cb.total_claim_amount,
    cb.total_settled_amount,
    cb.avg_claim_amount,
    cb.avg_settled_amount,
    
    -- Operational Metrics
    cb.avg_reporting_delay,
    cb.avg_settlement_ratio,
    cb.potential_fraud_cases,
    
    CASE 
        WHEN cb.claim_count > 0 
        THEN CAST(cb.potential_fraud_cases AS DOUBLE) / cb.claim_count
        ELSE 0
    END as fraud_rate,
    
    -- Adjuster Performance
    ap.total_claims_handled as adjuster_total_claims,
    ap.avg_settlement_ratio as adjuster_avg_settlement_ratio,
    ap.avg_processing_time as adjuster_avg_processing_time,
    ap.fraud_cases_detected as adjuster_fraud_detected,
    
    CASE 
        WHEN ap.avg_settlement_ratio >= 0.9 AND ap.avg_processing_time <= 7 THEN 'High Performer'
        WHEN ap.avg_settlement_ratio >= 0.8 AND ap.avg_processing_time <= 14 THEN 'Good Performer'
        WHEN ap.avg_settlement_ratio >= 0.7 AND ap.avg_processing_time <= 21 THEN 'Average Performer'
        ELSE 'Needs Improvement'
    END as adjuster_performance_tier,
    
    -- Geographic Analysis
    CASE 
        WHEN cb.customer_state IN ('FL', 'TX', 'CA') THEN 'High Risk State'
        WHEN cb.customer_state IN ('NY', 'NJ', 'CT') THEN 'Medium Risk State'
        ELSE 'Low Risk State'
    END as state_risk_category,
    
    -- Age-based Analysis
    CASE 
        WHEN cb.customer_age < 25 THEN 'Young Driver'
        WHEN cb.customer_age BETWEEN 25 AND 65 THEN 'Mature Driver'
        ELSE 'Senior Driver'
    END as customer_age_category,
    
    -- Trend Analysis
    LAG(cb.claim_count, 1) OVER (
        PARTITION BY cb.claim_type 
        ORDER BY cb.claim_month
    ) as prev_month_claims,
    
    LAG(cb.total_claim_amount, 1) OVER (
        PARTITION BY cb.claim_type 
        ORDER BY cb.claim_month
    ) as prev_month_claim_amount,
    
    -- Metadata
    CURRENT_DATE() as report_date

FROM claims_base cb
LEFT JOIN adjuster_performance ap ON cb.adjuster_id = ap.adjuster_id
