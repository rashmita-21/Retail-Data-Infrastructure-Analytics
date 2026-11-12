-- 11. Day-of-Week Sales Performance and Weekend vs Weekday Comparison
WITH dow_stats AS (
    SELECT
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        COUNT(DISTINCT d.full_date) AS active_calendar_days,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS completed_orders,
        ROUND(SUM(f.net_amount), 2) AS total_net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    GROUP BY d.day_of_week, d.day_name, d.is_weekend
)
SELECT
    day_of_week,
    day_name,
    is_weekend,
    active_calendar_days,
    completed_orders,
    total_net_revenue,
    ROUND(total_net_revenue / NULLIF(active_calendar_days, 0), 2) AS avg_daily_revenue,
    ROUND(total_net_revenue / NULLIF(completed_orders, 0), 2) AS avg_order_value
FROM dow_stats
ORDER BY day_of_week;
