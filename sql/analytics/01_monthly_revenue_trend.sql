-- 01. Monthly Revenue, Orders, and MoM Growth Trend
WITH monthly_metrics AS (
    SELECT
        d.year_month,
        COUNT(DISTINCT f.invoice_no) AS total_invoices,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN NOT c.is_guest THEN c.customer_id END) AS active_identified_customers,
        ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_revenue,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY d.year_month
)
SELECT
    year_month,
    total_invoices,
    completed_orders,
    active_identified_customers,
    gross_revenue,
    cancelled_revenue,
    net_revenue,
    LAG(net_revenue) OVER (ORDER BY year_month) AS prev_month_net_revenue,
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (ORDER BY year_month))
        / NULLIF(LAG(net_revenue) OVER (ORDER BY year_month), 0) * 100.0,
        2
    ) AS mom_net_revenue_growth_pct
FROM monthly_metrics
ORDER BY year_month;
