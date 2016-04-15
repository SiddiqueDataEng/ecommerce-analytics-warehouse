-- RFM Analysis Model
-- Calculates Recency, Frequency, Monetary scores for customer segmentation

{{
    config(
        materialized='table',
        tags=['analytics', 'rfm']
    )
}}

WITH customer_orders AS (
    SELECT
        c.customer_key,
        c.customer_id,
        o.order_date,
        o.order_total,
        o.order_id
    FROM {{ ref('dim_customer') }} c
    JOIN {{ ref('fact_orders') }} o ON c.customer_key = o.customer_key
    WHERE c.is_current = TRUE
        AND o.order_status = 'completed'
),

rfm_metrics AS (
    SELECT
        customer_key,
        customer_id,
        DATEDIFF(day, MAX(order_date), CURRENT_DATE()) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(order_total) AS monetary,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM customer_orders
    GROUP BY customer_key, customer_id
),

rfm_scores AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_metrics
),

rfm_segments AS (
    SELECT
        *,
        CONCAT(r_score, f_score, m_score) AS rfm_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost Customers'
            WHEN r_score >= 3 AND f_score <= 2 THEN 'Potential Loyalists'
            ELSE 'Regular Customers'
        END AS customer_segment,
        frequency AS total_orders,
        monetary AS total_revenue,
        monetary / NULLIF(frequency, 0) AS avg_order_value
    FROM rfm_scores
)

SELECT
    customer_key,
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_score,
    customer_segment,
    first_order_date,
    last_order_date,
    total_orders,
    total_revenue,
    avg_order_value,
    CURRENT_TIMESTAMP() AS updated_timestamp
FROM rfm_segments
