-- run_all.sql
.read sql/00_schema_checks.sql
.read sql/10_views_core.sql
.read sql/20_kpi_tables.sql
.read sql/30_customer_cohorts.sql

-- Quick peek at key outputs
SELECT * FROM kpi_monthly ORDER BY order_month DESC LIMIT 6;
SELECT * FROM kpi_by_region ORDER BY revenue DESC;
SELECT * FROM kpi_top_products ORDER BY revenue DESC LIMIT 10;
