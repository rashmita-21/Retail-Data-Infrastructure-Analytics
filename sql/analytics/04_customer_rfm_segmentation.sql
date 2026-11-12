-- 04. Customer RFM Segmentation (Recency, Frequency, Monetary) for Identified Customers
WITH reference_date AS (
    SELECT MAX(full_date) AS max_date FROM analytics.dim_date
),
customer_base AS (
    SELECT
        c.customer_id,
        c.primary_country,
        DATE_DIFF('day', MAX(d.full_date), (SELECT max_date FROM reference_date)) AS recency_days,
        COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) AS frequency,
        ROUND(SUM(f.net_amount), 2) AS monetary_net_spend
    FROM analytics.fact_sales f
    JOIN analytics.dim_customer c ON f.customer_key = c.customer_key
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    WHERE NOT c.is_guest
    GROUP BY c.customer_id, c.primary_country
    HAVING COUNT(DISTINCT CASE WHEN NOT f.is_cancelled THEN f.invoice_no END) > 0
       AND SUM(f.net_amount) > 0
),
rfm_scores AS (
    SELECT
        *,
        5 - NTILE(4) OVER (ORDER BY recency_days ASC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary_net_spend ASC) AS m_score
    FROM customer_base
),
segmented AS (
    SELECT
        *,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score <= 2 THEN 'Recent / Promising'
            WHEN r_score = 2 AND f_score >= 2 THEN 'At Risk'
            ELSE 'Hibernating / Needs Attention'
        END AS rfm_segment
    FROM rfm_scores
)
SELECT
    rfm_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(recency_days), 1) AS avg_recency_days,
    ROUND(AVG(frequency), 2) AS avg_frequency_orders,
    ROUND(AVG(monetary_net_spend), 2) AS avg_monetary_spend,
    ROUND(SUM(monetary_net_spend), 2) AS total_segment_revenue
FROM segmented
GROUP BY rfm_segment
ORDER BY total_segment_revenue DESC;
