{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'policy_performance'],
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'policy_performance' AS dataset,
          'gold' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_silver.policies_silver) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_gold.gold_policy_performance) tgt
    "]
) }}

WITH policy_base AS (
    SELECT 
        policy_type,
        status,
        payment_frequency,
        DATE_TRUNC('month', start_date) as policy_month,
        COUNT(*) as policy_count,
        SUM(premium_amount) as total_premium_revenue,
        SUM(coverage_amount) as total_coverage_exposure,
        AVG(premium_amount) as avg_premium,
        AVG(coverage_amount) as avg_coverage,
        AVG(deductible) as avg_deductible,
        AVG(policy_duration_days) as avg_policy_duration
    FROM {{ ref('policies_silver') }} p
    WHERE p.invalid_premium_amount_flag = 0 
      AND p.invalid_coverage_amount_flag = 0
      AND p.invalid_date_range_flag = 0
    GROUP BY policy_type, status, payment_frequency, DATE_TRUNC('month', start_date)
),

claims_by_policy_type AS (
    SELECT 
        p.policy_type,
        DATE_TRUNC('month', cl.claim_date) as claim_month,
        COUNT(DISTINCT cl.claim_id) as total_claims,
        SUM(cl.claim_amount) as total_claim_amount,
        SUM(cl.settled_amount) as total_settled_amount,
        AVG(cl.claim_amount) as avg_claim_severity,
        AVG(cl.reporting_delay_days) as avg_reporting_delay,
        COUNT(CASE WHEN cl.fraud_indicator = 1 THEN 1 END) as fraud_claims
    FROM {{ ref('claims_silver') }} cl
    JOIN {{ ref('policies_silver') }} p ON cl.policy_id = p.policy_id
    WHERE cl.missing_policy_flag = 0
    GROUP BY p.policy_type, DATE_TRUNC('month', cl.claim_date)
)

SELECT 
    pb.policy_type,
    pb.status,
    pb.payment_frequency,
    pb.policy_month,
    
    -- Volume Metrics
    pb.policy_count,
    pb.total_premium_revenue,
    pb.total_coverage_exposure,
    pb.avg_premium,
    pb.avg_coverage,
    pb.avg_deductible,
    pb.avg_policy_duration,
    
    -- Claims Metrics
    COALESCE(cp.total_claims, 0) as total_claims,
    COALESCE(cp.total_claim_amount, 0) as total_claim_amount,
    COALESCE(cp.total_settled_amount, 0) as total_settled_amount,
    COALESCE(cp.avg_claim_severity, 0) as avg_claim_severity,
    COALESCE(cp.avg_reporting_delay, 0) as avg_reporting_delay,
    COALESCE(cp.fraud_claims, 0) as fraud_claims,
    
    -- Key Performance Indicators
    CASE 
        WHEN pb.total_premium_revenue > 0 
        THEN COALESCE(cp.total_settled_amount, 0) / pb.total_premium_revenue
        ELSE 0
    END as loss_ratio,
    
    CASE 
        WHEN pb.policy_count > 0 
        THEN CAST(COALESCE(cp.total_claims, 0) AS DOUBLE) / pb.policy_count
        ELSE 0
    END as claims_frequency,
    
    pb.total_premium_revenue - COALESCE(cp.total_settled_amount, 0) as underwriting_profit,
    
    CASE 
        WHEN COALESCE(cp.total_claims, 0) > 0
        THEN CAST(cp.fraud_claims AS DOUBLE) / cp.total_claims
        ELSE 0
    END as fraud_rate,
    
    -- Performance Categorization
    CASE 
        WHEN COALESCE(cp.total_settled_amount, 0) / NULLIF(pb.total_premium_revenue, 0) <= 0.6 THEN 'Excellent'
        WHEN COALESCE(cp.total_settled_amount, 0) / NULLIF(pb.total_premium_revenue, 0) <= 0.8 THEN 'Good'
        WHEN COALESCE(cp.total_settled_amount, 0) / NULLIF(pb.total_premium_revenue, 0) <= 1.0 THEN 'Average'
        ELSE 'Poor'
    END as performance_tier,
    
    -- Growth Metrics (YoY)
    LAG(pb.total_premium_revenue, 12) OVER (
        PARTITION BY pb.policy_type 
        ORDER BY pb.policy_month
    ) as premium_revenue_prev_year,
    
    LAG(pb.policy_count, 12) OVER (
        PARTITION BY pb.policy_type 
        ORDER BY pb.policy_month
    ) as policy_count_prev_year,
    
    -- Metadata
    CURRENT_DATE() as report_date

FROM policy_base pb
LEFT JOIN claims_by_policy_type cp 
    ON pb.policy_type = cp.policy_type 
    AND pb.policy_month = cp.claim_month
