-- Databricks SQL, after running the notebook.
-- Grain: one row per month, channel and customer segment.
SELECT date_format(t.transaction_ts,'yyyy-MM') AS month,
       t.channel,c.segment,COUNT(*) AS transaction_count,
       SUM(CASE WHEN t.status='Failed' THEN 1 ELSE 0 END) AS failed_count,
       SUM(CASE WHEN t.status='Completed' THEN t.amount_inr ELSE 0 END) AS completed_value_inr,
       ROUND(AVG(CASE WHEN t.status='Completed' THEN t.processing_seconds END),1) AS avg_completed_seconds,
       ROUND(100.0*SUM(CASE WHEN t.status='Failed' THEN 1 ELSE 0 END)/COUNT(*),2) AS failure_rate_pct
FROM portfolio_transactions_clean t JOIN portfolio_customers c ON t.customer_id=c.customer_id
GROUP BY date_format(t.transaction_ts,'yyyy-MM'),t.channel,c.segment
ORDER BY month,channel,segment;

-- Reconciliation checks. Expected counts are in expected_metrics.json.
SELECT COUNT(*) AS clean_rows,COUNT(DISTINCT transaction_id) AS unique_ids FROM portfolio_transactions_clean;
SELECT COUNT(*) AS unmatched_customers FROM portfolio_transactions_clean t
LEFT ANTI JOIN portfolio_customers c ON t.customer_id=c.customer_id;
SELECT status,channel,COUNT(*) AS n FROM portfolio_transactions_clean GROUP BY status,channel;
