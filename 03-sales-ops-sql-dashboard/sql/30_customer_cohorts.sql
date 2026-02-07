-- 30_customer_cohorts.sql
-- Customer cohorts: first purchase month + repeat behavior

DROP TABLE IF EXISTS customer_first_order;
CREATE TABLE customer_first_order AS
SELECT
  customer_id,
  MIN(order_month) AS first_order_month
FROM v_orders_enriched
GROUP BY customer_id;

DROP TABLE IF EXISTS customer_cohorts;
CREATE TABLE customer_cohorts AS
WITH orders AS (
  SELECT
    o.customer_id,
    o.order_month,
    cfo.first_order_month
  FROM v_orders_enriched o
  JOIN customer_first_order cfo ON cfo.customer_id = o.customer_id
),
cohort_activity AS (
  SELECT
    first_order_month AS cohort_month,
    order_month AS activity_month,
    COUNT(DISTINCT customer_id) AS active_customers
  FROM orders
  GROUP BY first_order_month, order_month
),
cohort_size AS (
  SELECT
    cohort_month,
    MAX(active_customers) AS cohort_customers
  FROM (
    SELECT cohort_month, activity_month, active_customers FROM cohort_activity
  )
  GROUP BY cohort_month
)
SELECT
  ca.cohort_month,
  ca.activity_month,
  cs.cohort_customers,
  ca.active_customers,
  ROUND(ca.active_customers * 1.0 / cs.cohort_customers, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month
ORDER BY ca.cohort_month, ca.activity_month;
