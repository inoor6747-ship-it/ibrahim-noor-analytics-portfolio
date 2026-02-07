-- 00_schema_checks.sql
-- Quick sanity checks

SELECT 'dim_customers' AS table_name, COUNT(*) AS rows FROM dim_customers
UNION ALL SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_reps', COUNT(*) FROM dim_reps
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL SELECT 'fact_returns', COUNT(*) FROM fact_returns;

-- Date range
SELECT MIN(order_date) AS min_order_date, MAX(order_date) AS max_order_date FROM fact_orders;

-- Null checks (should be 0)
SELECT
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date
FROM fact_orders;
