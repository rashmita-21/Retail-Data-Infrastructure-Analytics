-- 07. Monthly Cancellation Impact on Gross vs Net Revenue
WITH monthly_cancellations AS (
    SELECT
        d.year_month,
        SUM(CASE WHEN NOT f.is_cancelled THEN 1 ELSE 0 END) AS positive_line_items,
        SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) AS cancelled_line_items,
        ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_revenue,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year_month
)
SELECT
    year_month,
    positive_line_items,
    cancelled_line_items,
    gross_revenue,
    cancelled_revenue,
    net_revenue,
    ROUND(cancelled_revenue * 100.0 / NULLIF(gross_revenue, 0), 2) AS cancellation_impact_pct,
    ROUND(AVG(cancelled_revenue * 100.0 / NULLIF(gross_revenue, 0)) OVER (), 2) AS overall_avg_impact_pct
FROM monthly_cancellations
ORDER BY year_month;
