-- Conversion Funnel Model
-- Tracks customer journey from visit to purchase

{{
    config(
        materialized='incremental',
        unique_key='funnel_date',
        tags=['analytics', 'conversion']
    )
}}

WITH daily_sessions AS (
    SELECT
        d.full_date AS funnel_date,
        COUNT(DISTINCT cb.session_key) AS total_sessions,
        COUNT(DISTINCT CASE WHEN cb.total_product_views > 0 THEN cb.session_key END) AS product_view_sessions,
        COUNT(DISTINCT CASE WHEN cb.total_add_to_cart > 0 THEN cb.session_key END) AS add_to_cart_sessions,
        COUNT(DISTINCT CASE WHEN cb.checkout_started THEN cb.session_key END) AS checkout_sessions,
        COUNT(DISTINCT CASE WHEN cb.purchase_completed THEN cb.session_key END) AS purchase_sessions
    FROM {{ ref('fact_customer_behavior') }} cb
    JOIN {{ ref('dim_date') }} d ON cb.date_key = d.date_key
    {% if is_incremental() %}
    WHERE d.full_date > (SELECT MAX(funnel_date) FROM {{ this }})
    {% endif %}
    GROUP BY d.full_date
),

funnel_stages AS (
    SELECT funnel_date, 'Visit' AS stage, 1 AS stage_order, total_sessions AS sessions FROM daily_sessions
    UNION ALL
    SELECT funnel_date, 'Product View' AS stage, 2 AS stage_order, product_view_sessions AS sessions FROM daily_sessions
    UNION ALL
    SELECT funnel_date, 'Add to Cart' AS stage, 3 AS stage_order, add_to_cart_sessions AS sessions FROM daily_sessions
    UNION ALL
    SELECT funnel_date, 'Checkout' AS stage, 4 AS stage_order, checkout_sessions AS sessions FROM daily_sessions
    UNION ALL
    SELECT funnel_date, 'Purchase' AS stage, 5 AS stage_order, purchase_sessions AS sessions FROM daily_sessions
),

funnel_with_rates AS (
    SELECT
        funnel_date,
        stage,
        stage_order,
        sessions,
        sessions / FIRST_VALUE(sessions) OVER (PARTITION BY funnel_date ORDER BY stage_order) AS conversion_rate,
        1 - (sessions / FIRST_VALUE(sessions) OVER (PARTITION BY funnel_date ORDER BY stage_order)) AS drop_off_rate
    FROM funnel_stages
)

SELECT
    funnel_date,
    stage,
    stage_order,
    sessions,
    conversion_rate,
    drop_off_rate
FROM funnel_with_rates
ORDER BY funnel_date, stage_order
