-- 13. Repeat vs One-Time Identified Customer Revenue Contribution
WITH customer_orders AS (
    SELECT
        c.customer_key,
        COUNT(DISTINCT f.invoice_no) AS completed_orders,
        ROUND(SUM(f.net_amount), 2) AS net_spend
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    WHERE NOT c.is_guest AND NOT f.is_cancelled
    GROUP BY c.customer_key
),
classified AS (
    SELECT
        CASE
            WHEN completed_orders = 1 THEN '1_One-Time (1 order)'
            WHEN completed_orders BETWEEN 2 AND 4 THEN '2_Repeat (2-4 orders)'
            WHEN completed_orders BETWEEN 5 AND 10 THEN '3_Frequent (5-10 orders)'
            ELSE '4_VIP (11+ orders)'
        END AS loyalty_tier,
        completed_orders,
        net_spend
    FROM customer_orders
)
SELECT
    loyalty_tier,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers,
    SUM(completed_orders) AS total_orders,
    ROUND(SUM(net_spend), 2) AS total_revenue,
    ROUND(SUM(net_spend) * 100.0 / SUM(SUM(net_spend)) OVER (), 2) AS pct_of_revenue,
    ROUND(AVG(net_spend), 2) AS avg_customer_spend
FROM classified
GROUP BY loyalty_tier
ORDER BY loyalty_tier;
