-- 09. Daily Revenue with 7-Day Rolling Moving Average and Cumulative YTD Revenue
WITH daily_sales AS (
    SELECT
        d.full_date,
        d.year,
        d.day_name,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS completed_orders,
        ROUND(SUM(f.net_amount), 2) AS daily_net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    GROUP BY d.full_date, d.year, d.day_name
)
SELECT
    full_date,
    day_name,
    completed_orders,
    daily_net_revenue,
    ROUND(
        AVG(daily_net_revenue) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        2
    ) AS rolling_7d_avg_revenue,
    ROUND(
        SUM(daily_net_revenue) OVER (PARTITION BY year ORDER BY full_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        2
    ) AS cumulative_ytd_revenue
FROM daily_sales
ORDER BY full_date;
