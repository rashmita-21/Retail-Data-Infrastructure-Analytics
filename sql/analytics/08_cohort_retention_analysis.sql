-- 08. Monthly Customer Cohort Retention Analysis
WITH customer_cohorts AS (
    SELECT
        c.customer_key,
        DATE_TRUNC('month', MIN(d.full_date))::DATE AS cohort_month
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    WHERE NOT c.is_guest AND NOT f.is_cancelled
    GROUP BY c.customer_key
),
customer_activity AS (
    SELECT DISTINCT
        f.customer_key,
        DATE_TRUNC('month', d.full_date)::DATE AS activity_month
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    WHERE NOT f.is_cancelled
),
cohort_offsets AS (
    SELECT
        cc.cohort_month,
        DATE_DIFF('month', cc.cohort_month, ca.activity_month) AS months_since_first_purchase,
        COUNT(DISTINCT cc.customer_key) AS active_customers
    FROM customer_cohorts cc
    JOIN customer_activity ca ON cc.customer_key = ca.customer_key
    GROUP BY cc.cohort_month, months_since_first_purchase
),
cohort_sizes AS (
    SELECT
        cohort_month,
        active_customers AS cohort_size
    FROM cohort_offsets
    WHERE months_since_first_purchase = 0
)
SELECT
    STRFTIME(co.cohort_month, '%Y-%m') AS cohort_month,
    cs.cohort_size,
    co.months_since_first_purchase,
    co.active_customers,
    ROUND(co.active_customers * 100.0 / NULLIF(cs.cohort_size, 0), 2) AS retention_rate_pct
FROM cohort_offsets co
JOIN cohort_sizes cs ON co.cohort_month = cs.cohort_month
WHERE co.months_since_first_purchase BETWEEN 0 AND 6
ORDER BY co.cohort_month, co.months_since_first_purchase;
