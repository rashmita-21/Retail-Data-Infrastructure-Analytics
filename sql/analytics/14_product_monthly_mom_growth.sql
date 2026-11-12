-- 14. Top 5 Products Monthly MoM Revenue Trend (Top 5 Overall Products)
WITH top5_products AS (
    SELECT p.product_key, p.stock_code, p.product_description
    FROM analytics.fact_sales f
    JOIN analytics.dim_product p ON f.product_key = p.product_key
    WHERE NOT p.is_non_product_fee
    GROUP BY p.product_key, p.stock_code, p.product_description
    ORDER BY SUM(f.net_amount) DESC
    LIMIT 5
),
product_monthly AS (
    SELECT
        t.stock_code,
        t.product_description,
        d.year_month,
        ROUND(SUM(f.net_amount), 2) AS monthly_net_revenue
    FROM analytics.fact_sales f
    JOIN top5_products t ON f.product_key = t.product_key
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    GROUP BY t.stock_code, t.product_description, d.year_month
)
SELECT
    stock_code,
    product_description,
    year_month,
    monthly_net_revenue,
    LAG(monthly_net_revenue) OVER (PARTITION BY stock_code ORDER BY year_month) AS prev_month_revenue,
    ROUND(
        (monthly_net_revenue - LAG(monthly_net_revenue) OVER (PARTITION BY stock_code ORDER BY year_month))
        / NULLIF(LAG(monthly_net_revenue) OVER (PARTITION BY stock_code ORDER BY year_month), 0) * 100.0,
        2
    ) AS mom_growth_pct
FROM product_monthly
ORDER BY stock_code, year_month;
