# Project 3 — Sales Operations Analytics (SQL + Power BI)

## Overview
This project analyzes sales operations performance using a SQL-based reporting layer and an executive Power BI dashboard. The focus is on revenue trends, channel and regional performance, return behavior, and product-level drivers.

The goal is to demonstrate end-to-end analytics skills:
- SQL data modeling and KPI generation
- Business-focused metric design
- Executive dashboard storytelling

---

## Data Pipeline
- Raw order and return data stored in SQLite
- SQL transformations generate reporting tables:
  - Monthly revenue, orders, AOV, margins
  - Revenue by region and channel
  - Return rates by channel
  - Top products by revenue and margin
- KPI tables exported to CSV for BI consumption

---

## Key Metrics
- Revenue
- Orders
- Average Order Value (AOV)
- Gross Margin %
- Return Rate by Channel
- Revenue by Region and Channel
- Top Products by Revenue

---

## Dashboard Features
- Interactive month-based KPI cards
- Revenue trend analysis
- Channel and regional performance breakdown
- Return rate monitoring by channel
- Product-level revenue and margin insights
- Dynamic slicers for month, channel, and region

---

## Tools Used
- SQL (SQLite)
- DB Browser for SQLite
- Power BI
- GitHub

---

## Files
- `data/` — SQLite database and KPI CSV exports
- `powerbi/sales_ops_dashboard.pbix` — Power BI report
- `powerbi/sales_ops_dashboard.pdf` — Exported dashboard
- `src/` — SQL scripts used to generate KPIs
