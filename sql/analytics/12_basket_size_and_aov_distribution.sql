-- 12. Basket Size and Average Order Value (AOV) Distribution by Customer Type
WITH invoice_baskets AS (
    SELECT
        f.invoice_no,
        c.is_guest,
        COUNT(*) AS distinct_line_items,
        SUM(f.quantity) AS total_units,
        ROUND(SUM(f.net_amount), 2) AS invoice_value
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    WHERE NOT f.is_cancelled
    GROUP BY f.invoice_no, c.is_guest
)
SELECT
    CASE WHEN is_guest THEN 'Guest / Unidentified' ELSE 'Identified Customer' END AS customer_type,
    COUNT(*) AS total_orders,
    ROUND(AVG(distinct_line_items), 2) AS avg_lines_per_basket,
    ROUND(AVG(total_units), 2) AS avg_units_per_basket,
    ROUND(AVG(invoice_value), 2) AS mean_aov,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY invoice_value), 2) AS median_aov,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY invoice_value), 2) AS p90_aov,
    ROUND(SUM(invoice_value), 2) AS total_gross_sales
FROM invoice_baskets
GROUP BY is_guest
ORDER BY total_gross_sales DESC;
