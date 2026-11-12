-- 06. Cancellation Rate Analysis by Country (Countries with >= 50 Invoices)
WITH country_cancel AS (
    SELECT
        co.country_name,
        COUNT(DISTINCT f.invoice_no) AS total_invoices,
        COUNT(DISTINCT CASE WHEN f.is_cancelled THEN f.invoice_no END) AS cancelled_invoices,
        ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_revenue,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_country co ON f.country_key = co.country_key
    GROUP BY co.country_name
    HAVING COUNT(DISTINCT f.invoice_no) >= 50
)
SELECT
    country_name,
    total_invoices,
    cancelled_invoices,
    ROUND(cancelled_invoices * 100.0 / NULLIF(total_invoices, 0), 2) AS invoice_cancellation_rate_pct,
    gross_revenue,
    cancelled_revenue,
    ROUND(cancelled_revenue * 100.0 / NULLIF(gross_revenue, 0), 2) AS revenue_cancellation_rate_pct,
    net_revenue
FROM country_cancel
ORDER BY revenue_cancellation_rate_pct DESC;
