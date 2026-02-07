-- 20_kpi_tables.sql
-- KPI tables for dashboarding

DROP TABLE IF EXISTS kpi_monthly;
CREATE TABLE kpi_monthly AS
WITH item_month AS (
  SELECT
    order_month,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(revenue) AS revenue,
    SUM(cost) AS cost,
    SUM(gross_profit) AS gross_profit
  FROM v_order_items_enriched
  GROUP BY order_month
),
returns_month AS (
  SELECT
    order_month,
    COUNT(DISTINCT order_id) AS returned_orders
  FROM v_returns_enriched
  GROUP BY order_month
)
SELECT
  im.order_month,
  im.orders,
  im.customers,
  ROUND(im.revenue, 2) AS revenue,
  ROUND(im.gross_profit, 2) AS gross_profit,
  ROUND(CASE WHEN im.revenue = 0 THEN 0 ELSE im.gross_profit / im.revenue END, 4) AS gross_margin_rate,
  ROUND(CASE WHEN im.orders = 0 THEN 0 ELSE im.revenue / im.orders END, 2) AS aov,
  COALESCE(rm.returned_orders, 0) AS returned_orders,
  ROUND(CASE WHEN im.orders = 0 THEN 0 ELSE COALESCE(rm.returned_orders, 0) * 1.0 / im.orders END, 4) AS return_rate
FROM item_month im
LEFT JOIN returns_month rm
  ON rm.order_month = im.order_month;

DROP TABLE IF EXISTS kpi_by_region;
CREATE TABLE kpi_by_region AS
SELECT
  rep_region AS region,
  COUNT(DISTINCT order_id) AS orders,
  SUM(revenue) AS revenue,
  SUM(gross_profit) AS gross_profit,
  ROUND(CASE WHEN SUM(revenue) = 0 THEN 0 ELSE SUM(gross_profit) / SUM(revenue) END, 4) AS gross_margin_rate,
  ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS aov
FROM v_order_items_enriched
GROUP BY rep_region;

DROP TABLE IF EXISTS kpi_by_channel;
CREATE TABLE kpi_by_channel AS
SELECT
  channel,
  COUNT(DISTINCT order_id) AS orders,
  SUM(revenue) AS revenue,
  SUM(gross_profit) AS gross_profit,
  ROUND(CASE WHEN SUM(revenue) = 0 THEN 0 ELSE SUM(gross_profit) / SUM(revenue) END, 4) AS gross_margin_rate,
  ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS aov
FROM v_order_items_enriched
GROUP BY channel;

DROP TABLE IF EXISTS kpi_top_products;
CREATE TABLE kpi_top_products AS
SELECT
  product_id,
  product_name,
  category,
  SUM(quantity) AS units,
  SUM(revenue) AS revenue,
  SUM(gross_profit) AS gross_profit,
  ROUND(CASE WHEN SUM(revenue) = 0 THEN 0 ELSE SUM(gross_profit) / SUM(revenue) END, 4) AS gross_margin_rate
FROM v_order_items_enriched
GROUP BY product_id, product_name, category
ORDER BY revenue DESC
LIMIT 25;

DROP TABLE IF EXISTS kpi_returns_by_reason;
CREATE TABLE kpi_returns_by_reason AS
SELECT
  reason,
  COUNT(*) AS returns
FROM v_returns_enriched
GROUP BY reason
ORDER BY returns DESC;

DROP TABLE IF EXISTS kpi_returns_by_channel;
CREATE TABLE kpi_returns_by_channel AS
WITH orders AS (
  SELECT order_month, channel, COUNT(DISTINCT order_id) AS orders
  FROM v_orders_enriched
  GROUP BY order_month, channel
),
rets AS (
  SELECT order_month, channel, COUNT(DISTINCT order_id) AS returned_orders
  FROM v_returns_enriched
  GROUP BY order_month, channel
)
SELECT
  o.order_month,
  o.channel,
  o.orders,
  COALESCE(r.returned_orders, 0) AS returned_orders,
  ROUND(CASE WHEN o.orders = 0 THEN 0 ELSE COALESCE(r.returned_orders, 0) * 1.0 / o.orders END, 4) AS return_rate
FROM orders o
LEFT JOIN rets r
  ON r.order_month = o.order_month AND r.channel = o.channel
ORDER BY o.order_month, o.channel;
