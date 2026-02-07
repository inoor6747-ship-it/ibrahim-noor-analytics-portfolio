-- 10_views_core.sql
-- Core views used by KPI tables and Power BI

DROP VIEW IF EXISTS v_order_items_enriched;
CREATE VIEW v_order_items_enriched AS
SELECT
  oi.order_item_id,
  oi.order_id,
  o.order_date,
  substr(o.order_date, 1, 7) AS order_month,
  o.channel,
  o.discount_rate,
  o.customer_id,
  c.customer_name,
  c.segment,
  c.region AS customer_region,
  o.rep_id,
  r.rep_name,
  r.region AS rep_region,
  oi.product_id,
  p.product_name,
  p.category,
  oi.quantity,
  oi.unit_price,
  oi.unit_cost,
  ROUND(oi.quantity * oi.unit_price, 2) AS revenue,
  ROUND(oi.quantity * oi.unit_cost, 2) AS cost,
  ROUND((oi.quantity * oi.unit_price) - (oi.quantity * oi.unit_cost), 2) AS gross_profit
FROM fact_order_items oi
JOIN fact_orders o ON o.order_id = oi.order_id
JOIN dim_customers c ON c.customer_id = o.customer_id
JOIN dim_reps r ON r.rep_id = o.rep_id
JOIN dim_products p ON p.product_id = oi.product_id;

DROP VIEW IF EXISTS v_orders_enriched;
CREATE VIEW v_orders_enriched AS
SELECT
  o.order_id,
  o.order_date,
  substr(o.order_date, 1, 7) AS order_month,
  o.channel,
  o.discount_rate,
  o.customer_id,
  c.customer_name,
  c.segment,
  c.region AS customer_region,
  o.rep_id,
  r.rep_name,
  r.region AS rep_region
FROM fact_orders o
JOIN dim_customers c ON c.customer_id = o.customer_id
JOIN dim_reps r ON r.rep_id = o.rep_id;

DROP VIEW IF EXISTS v_returns_enriched;
CREATE VIEW v_returns_enriched AS
SELECT
  ret.return_id,
  ret.order_id,
  ret.return_date,
  substr(ret.return_date, 1, 7) AS return_month,
  ret.reason,
  o.order_date,
  substr(o.order_date, 1, 7) AS order_month,
  o.channel,
  o.customer_id,
  c.segment,
  c.region AS customer_region,
  o.rep_id,
  r.region AS rep_region
FROM fact_returns ret
JOIN fact_orders o ON o.order_id = ret.order_id
JOIN dim_customers c ON c.customer_id = o.customer_id
JOIN dim_reps r ON r.rep_id = o.rep_id;
