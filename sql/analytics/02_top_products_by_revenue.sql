-- 02. Top 20 Products by Net Revenue (Excluding Non-Product Fees)
WITH product_stats AS (
    SELECT
        p.stock_code,
        p.product_description,
        SUM(CASE WHEN NOT f.is_cancelled THEN f.quantity ELSE 0 END) AS units_sold,
        SUM(CASE WHEN f.is_cancelled THEN f.abs_quantity ELSE 0 END) AS units_cancelled,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS order_count,
        ROUND(SUM(f.gross_amount), 2) AS gross_revenue,
        ROUND(SUM(f.cancelled_amount), 2) AS cancelled_revenue,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_product p ON f.product_key = p.product_key
    WHERE NOT p.is_non_product_fee
    GROUP BY p.stock_code, p.product_description
)
SELECT
    DENSE_RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank,
    stock_code,
    product_description,
    units_sold,
    units_cancelled,
    order_count,
    gross_revenue,
    cancelled_revenue,
    net_revenue,
    ROUND(net_revenue / NULLIF(SUM(net_revenue) OVER (), 0) * 100.0, 3) AS pct_of_total_net_revenue
FROM product_stats
ORDER BY revenue_rank
LIMIT 20;
