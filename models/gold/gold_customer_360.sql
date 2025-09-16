{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'customer_analytics'],
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'customer_360' AS dataset,
          'gold' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_silver.customers_silver) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_gold.gold_customer_360) tgt
    "]
) }}

WITH customer_base AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.age,
        c.annual_income,
        c.credit_score,
        c.state,
        c.marital_status,
        c.occupation,
        c.processed_at
    FROM {{ ref('customers_silver') }} c
    WHERE c.invalid_email_flag = 0
),

policy_metrics AS (
    SELECT 
        p.customer_id,
        COUNT(DISTINCT p.policy_id) as total_policies,
        SUM(p.premium_amount) as total_annual_premium,
        AVG(p.premium_amount) as avg_policy_premium,
        SUM(p.coverage_amount) as total_coverage,
        AVG(p.coverage_amount) as avg_coverage,
        MIN(p.start_date) as first_policy_date,
        MAX(p.end_date) as latest_policy_end,
        COUNT(CASE WHEN p.status = 'ACTIVE' THEN 1 END) as active_policies,
        COUNT(CASE WHEN p.status = 'CANCELLED' THEN 1 END) as cancelled_policies
    FROM {{ ref('policies_silver') }} p
    WHERE p.missing_customer_id_flag = 0 
      AND p.invalid_premium_amount_flag = 0
    GROUP BY p.customer_id
),

claims_metrics AS (
    SELECT 
        p.customer_id,
        COUNT(DISTINCT cl.claim_id) as total_claims,
        SUM(cl.claim_amount) as total_claimed,
        SUM(cl.settled_amount) as total_settled,
        AVG(cl.claim_amount) as avg_claim_amount,
        AVG(cl.reporting_delay_days) as avg_reporting_delay,
        COUNT(CASE WHEN cl.fraud_indicator = 1 THEN 1 END) as potential_fraud_claims,
        MIN(cl.claim_date) as first_claim_date,
        MAX(cl.claim_date) as latest_claim_date
    FROM {{ ref('claims_silver') }} cl
    JOIN {{ ref('policies_silver') }} p ON cl.policy_id = p.policy_id
    WHERE cl.missing_policy_flag = 0
    GROUP BY p.customer_id
)

SELECT 
    cb.customer_id,
    cb.full_name,
    cb.age,
    CASE 
        WHEN cb.age < 25 THEN 'Gen Z'
        WHEN cb.age < 35 THEN 'Millennial'
        WHEN cb.age < 55 THEN 'Gen X'
        ELSE 'Boomer+'
    END as generation_segment,
    cb.annual_income,
    cb.credit_score,
    cb.state,
    cb.marital_status,
    cb.occupation,
    
    -- Policy Metrics
    COALESCE(pm.total_policies, 0) as total_policies,
    COALESCE(pm.total_annual_premium, 0) as total_annual_premium,
    COALESCE(pm.avg_policy_premium, 0) as avg_policy_premium,
    COALESCE(pm.total_coverage, 0) as total_coverage,
    COALESCE(pm.active_policies, 0) as active_policies,
    COALESCE(pm.cancelled_policies, 0) as cancelled_policies,
    pm.first_policy_date,
    pm.latest_policy_end,
    
    -- Claims Metrics
    COALESCE(cm.total_claims, 0) as total_claims,
    COALESCE(cm.total_claimed, 0) as total_claimed,
    COALESCE(cm.total_settled, 0) as total_settled,
    COALESCE(cm.avg_claim_amount, 0) as avg_claim_amount,
    COALESCE(cm.potential_fraud_claims, 0) as potential_fraud_claims,
    
    -- Business KPIs
    COALESCE(pm.total_annual_premium, 0) - COALESCE(cm.total_settled, 0) as customer_lifetime_profit,
    
    CASE 
        WHEN COALESCE(pm.total_annual_premium, 0) = 0 THEN 0
        ELSE COALESCE(cm.total_settled, 0) / pm.total_annual_premium
    END as loss_ratio,
    
    CASE 
        WHEN COALESCE(pm.total_policies, 0) = 0 THEN 0
        ELSE CAST(COALESCE(cm.total_claims, 0) AS DOUBLE) / pm.total_policies
    END as claims_frequency,
    
    -- Customer Segmentation
    CASE 
        WHEN COALESCE(pm.total_annual_premium, 0) - COALESCE(cm.total_settled, 0) > 10000 THEN 'High Value'
        WHEN COALESCE(pm.total_annual_premium, 0) - COALESCE(cm.total_settled, 0) > 5000 THEN 'Medium Value'
        WHEN COALESCE(pm.total_annual_premium, 0) - COALESCE(cm.total_settled, 0) > 0 THEN 'Low Value'
        ELSE 'Loss Making'
    END as customer_value_segment,
    
    CASE 
        WHEN COALESCE(cm.total_claims, 0) = 0 THEN 'No Claims'
        WHEN CAST(COALESCE(cm.total_claims, 0) AS DOUBLE) / COALESCE(pm.total_policies, 1) < 0.1 THEN 'Low Risk'
        WHEN CAST(COALESCE(cm.total_claims, 0) AS DOUBLE) / COALESCE(pm.total_policies, 1) < 0.3 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_tier,
    
    -- Churn Indicators
    CASE 
        WHEN pm.latest_policy_end < CURRENT_DATE() THEN 1
        ELSE 0
    END as churned_flag,
    
    CASE 
        WHEN COALESCE(cm.potential_fraud_claims, 0) > 0 THEN 1
        ELSE 0
    END as fraud_flag,
    
    -- Metadata
    CURRENT_DATE() as report_date,
    cb.processed_at

FROM customer_base cb
LEFT JOIN policy_metrics pm ON cb.customer_id = pm.customer_id
LEFT JOIN claims_metrics cm ON cb.customer_id = cm.customer_id
