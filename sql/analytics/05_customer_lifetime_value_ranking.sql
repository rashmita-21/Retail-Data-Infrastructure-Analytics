-- 05. Top 25 Customers by Lifetime Value (CLV) with Country Rank
WITH customer_clv AS (
    SELECT
        c.customer_id,
        c.primary_country,
        MIN(f.invoice_ts) AS first_purchase_ts,
        MAX(f.invoice_ts) AS last_purchase_ts,
        DATE_DIFF('day', MIN(f.invoice_ts)::DATE, MAX(f.invoice_ts)::DATE) + 1 AS active_span_days,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN f.is_cancelled THEN f.invoice_no END) AS cancelled_orders,
        ROUND(SUM(f.gross_amount), 2) AS gross_spend,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_spend,
        ROUND(SUM(f.net_amount), 2) AS net_lifetime_value
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    WHERE NOT c.is_guest
    GROUP BY c.customer_id, c.primary_country
)
SELECT
    DENSE_RANK() OVER (ORDER BY net_lifetime_value DESC) AS global_clv_rank,
    DENSE_RANK() OVER (PARTITION BY primary_country ORDER BY net_lifetime_value DESC) AS country_clv_rank,
    customer_id,
    primary_country,
    active_span_days,
    completed_orders,
    cancelled_orders,
    gross_spend,
    cancelled_spend,
    net_lifetime_value,
    ROUND(net_lifetime_value / NULLIF(completed_orders, 0), 2) AS avg_order_value
FROM customer_clv
ORDER BY global_clv_rank
LIMIT 25;
