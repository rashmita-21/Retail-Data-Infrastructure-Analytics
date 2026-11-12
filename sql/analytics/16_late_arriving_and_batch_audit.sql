-- 16. Warehouse Batch & Late-Arriving Data Audit
SELECT
    a.batch_id,
    a.batch_type,
    a.is_late_arriving,
    a.rows_loaded,
    a.min_invoice_ts,
    a.max_invoice_ts,
    COUNT(f.transaction_line_id) AS verified_fact_rows,
    ROUND(SUM(f.net_amount), 2) AS batch_net_revenue,
    a.loaded_at
FROM analytics.etl_batch_audit a
LEFT JOIN analytics.fact_sales f ON a.batch_id = f.batch_id
GROUP BY
    a.batch_id,
    a.batch_type,
    a.is_late_arriving,
    a.rows_loaded,
    a.min_invoice_ts,
    a.max_invoice_ts,
    a.loaded_at
ORDER BY a.batch_id;
