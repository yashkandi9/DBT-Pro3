{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'executive_summary'],
    post_hook=["
        INSERT INTO insurance_catalog.dbt_ykandi_logging.dbt_logs
        (dataset, layer, time_processed, source_records, target_records, bad_records)
        SELECT
          'executive_summary' AS dataset,
          'gold' AS layer,
          CURRENT_TIMESTAMP AS time_processed,
          src.source_count,
          tgt.target_count,
          0 AS bad_records
        FROM
          (SELECT COUNT(*) AS source_count FROM insurance_catalog.dbt_ykandi_silver.policies_silver) src,
          (SELECT COUNT(*) AS target_count FROM insurance_catalog.dbt_ykandi_gold.gold_executive_summary) tgt
    "]
) }}

WITH monthly_financials AS (
    SELECT 
        DATE_TRUNC('month', p.start_date) as report_period,
        COUNT(DISTINCT p.policy_id) as new_policies,
        COUNT(DISTINCT p.customer_id) as active_customers,
        SUM(p.premium_amount) as gross_written_premium,
        SUM(p.coverage_amount) as total_coverage_in_force,
        AVG(p.premium_amount) as avg_policy_premium
    FROM {{ ref('policies_silver') }} p
    WHERE p.invalid_premium_amount_flag = 0
    GROUP BY DATE_TRUNC('month', p.start_date)
),

monthly_claims AS (
    SELECT 
        DATE_TRUNC('month', cl.claim_date) as report_period,
        COUNT(DISTINCT cl.claim_id) as total_claims,
        SUM(cl.claim_amount) as total_incurred_claims,
        SUM(cl.settled_amount) as total_paid_claims,
        AVG(cl.claim_amount) as avg_claim_severity,
        COUNT(CASE WHEN cl.fraud_indicator = 1 THEN 1 END) as fraud_claims
    FROM {{ ref('claims_silver') }} cl
    WHERE cl.missing_policy_flag = 0
    GROUP BY DATE_TRUNC('month', cl.claim_date)
),

kpis AS (
    SELECT 
        COALESCE(mf.report_period, mc.report_period) as report_period,
        
        -- Premium Metrics
        COALESCE(mf.new_policies, 0) as new_policies,
        COALESCE(mf.active_customers, 0) as active_customers,
        COALESCE(mf.gross_written_premium, 0) as gross_written_premium,
        COALESCE(mf.total_coverage_in_force, 0) as total_coverage_in_force,
        COALESCE(mf.avg_policy_premium, 0) as avg_policy_premium,
        
        -- Claims Metrics
        COALESCE(mc.total_claims, 0) as total_claims,
        COALESCE(mc.total_incurred_claims, 0) as total_incurred_claims,
        COALESCE(mc.total_paid_claims, 0) as total_paid_claims,
        COALESCE(mc.avg_claim_severity, 0) as avg_claim_severity,
        COALESCE(mc.fraud_claims, 0) as fraud_claims,
        
        -- Key Ratios
        CASE 
            WHEN COALESCE(mf.gross_written_premium, 0) > 0
            THEN COALESCE(mc.total_paid_claims, 0) / mf.gross_written_premium
            ELSE 0
        END as loss_ratio,
        
        CASE 
            WHEN COALESCE(mf.new_policies, 0) > 0
            THEN CAST(COALESCE(mc.total_claims, 0) AS DOUBLE) / mf.new_policies
            ELSE 0
        END as claims_frequency,
        
        -- Profitability
        COALESCE(mf.gross_written_premium, 0) - COALESCE(mc.total_paid_claims, 0) as underwriting_profit,
        
        CASE 
            WHEN COALESCE(mc.total_claims, 0) > 0
            THEN CAST(mc.fraud_claims AS DOUBLE) / mc.total_claims
            ELSE 0
        END as fraud_rate
        
    FROM monthly_financials mf
    FULL OUTER JOIN monthly_claims mc ON mf.report_period = mc.report_period
)

SELECT 
    report_period,
    
    -- Volume KPIs
    new_policies,
    active_customers,
    total_claims,
    
    -- Financial KPIs
    gross_written_premium,
    total_coverage_in_force,
    total_incurred_claims,
    total_paid_claims,
    underwriting_profit,
    
    -- Performance Ratios
    loss_ratio,
    claims_frequency,
    fraud_rate,
    avg_policy_premium,
    avg_claim_severity,
    
    -- Growth Metrics (YoY)
    LAG(gross_written_premium, 12) OVER (ORDER BY report_period) as gwp_prev_year,
    LAG(new_policies, 12) OVER (ORDER BY report_period) as policies_prev_year,
    LAG(underwriting_profit, 12) OVER (ORDER BY report_period) as profit_prev_year,
    
    -- Calculate Growth Rates
    CASE 
        WHEN LAG(gross_written_premium, 12) OVER (ORDER BY report_period) > 0
        THEN (gross_written_premium - LAG(gross_written_premium, 12) OVER (ORDER BY report_period)) 
             / LAG(gross_written_premium, 12) OVER (ORDER BY report_period) * 100
        ELSE 0
    END as gwp_growth_rate_yoy,
    
    CASE 
        WHEN LAG(new_policies, 12) OVER (ORDER BY report_period) > 0
        THEN (new_policies - LAG(new_policies, 12) OVER (ORDER BY report_period))
             / CAST(LAG(new_policies, 12) OVER (ORDER BY report_period) AS DOUBLE) * 100
        ELSE 0
    END as policy_growth_rate_yoy,
    
    -- Performance Categories
    CASE 
        WHEN loss_ratio <= 0.6 THEN 'Excellent'
        WHEN loss_ratio <= 0.8 THEN 'Good' 
        WHEN loss_ratio <= 1.0 THEN 'Acceptable'
        ELSE 'Concerning'
    END as performance_category,
    
    -- Metadata
    CURRENT_DATE() as report_date,
    CURRENT_TIMESTAMP() as created_at

FROM kpis
ORDER BY report_period DESC
