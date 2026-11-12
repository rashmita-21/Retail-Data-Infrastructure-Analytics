-- 15. Pareto 80/20 Product Revenue Concentration Analysis
WITH product_rev AS (
    SELECT
        p.stock_code,
        p.product_description,
        ROUND(SUM(f.net_amount), 2) AS net_revenue
    FROM analytics.fact_sales f
    JOIN analytics.dim_product p ON f.product_key = p.product_key
    WHERE NOT p.is_non_product_fee
    GROUP BY p.stock_code, p.product_description
    HAVING SUM(f.net_amount) > 0
),
ranked AS (
    SELECT
        stock_code,
        product_description,
        net_revenue,
        ROW_NUMBER() OVER (ORDER BY net_revenue DESC) AS product_rank,
        COUNT(*) OVER () AS total_products,
        SUM(net_revenue) OVER (ORDER BY net_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
        SUM(net_revenue) OVER () AS total_revenue
    FROM product_rev
)
SELECT
    CASE
        WHEN cumulative_revenue / total_revenue <= 0.80 THEN 'Top 80% Revenue Drivers'
        WHEN cumulative_revenue / total_revenue <= 0.95 THEN 'Next 15% Long-Tail'
        ELSE 'Bottom 5% Niche'
    END AS pareto_bucket,
    COUNT(*) AS sku_count,
    ROUND(COUNT(*) * 100.0 / MAX(total_products), 2) AS pct_of_skus,
    ROUND(SUM(net_revenue), 2) AS bucket_revenue,
    ROUND(SUM(net_revenue) * 100.0 / MAX(total_revenue), 2) AS pct_of_revenue
FROM ranked
GROUP BY pareto_bucket
ORDER BY bucket_revenue DESC;
