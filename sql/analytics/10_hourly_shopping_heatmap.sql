-- 10. Hourly Shopping Activity and Revenue Concentration
WITH hourly_stats AS (
    SELECT
        f.invoice_hour,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS completed_orders,
        SUM(CASE WHEN NOT f.is_cancelled THEN f.quantity ELSE 0 END) AS units_sold,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    GROUP BY f.invoice_hour
)
SELECT
    invoice_hour,
    completed_orders,
    units_sold,
    net_revenue,
    ROUND(net_revenue * 100.0 / NULLIF(SUM(net_revenue) OVER (), 0), 2) AS share_of_revenue_pct,
    RANK() OVER (ORDER BY net_revenue DESC) AS hour_revenue_rank
FROM hourly_stats
ORDER BY invoice_hour;
