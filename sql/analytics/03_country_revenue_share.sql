-- 03. Country Revenue Share and Regional Performance Ranking
WITH country_totals AS (
    SELECT
        co.country_name,
        co.region_group,
        COUNT(DISTINCT f.invoice_no) AS total_invoices,
        COUNT(DISTINCT CASE WHEN NOT c.is_guest THEN c.customer_id END) AS unique_customers,
        ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_revenue,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_country co ON f.country_key = co.country_key
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY co.country_name, co.region_group
)
SELECT
    RANK() OVER (ORDER BY net_revenue DESC) AS country_rank,
    country_name,
    region_group,
    total_invoices,
    unique_customers,
    gross_revenue,
    cancelled_revenue,
    net_revenue,
    ROUND(net_revenue / NULLIF(SUM(net_revenue) OVER (), 0) * 100.0, 2) AS revenue_share_pct,
    ROUND(
        SUM(net_revenue) OVER (ORDER BY net_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / NULLIF(SUM(net_revenue) OVER (), 0) * 100.0,
        2
    ) AS cumulative_revenue_share_pct
FROM country_totals
ORDER BY country_rank;
