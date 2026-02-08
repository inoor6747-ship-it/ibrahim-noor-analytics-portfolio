# Data Quality Governance Dashboard

## Business Problem
Organizations rely on data that may contain missing values, duplicates, and invalid records. Without visibility into data quality issues, reporting accuracy and operational decisions are at risk. This project builds a data quality scorecard to monitor and surface data quality problems in a consistent, measurable way.

## What I Built
- Rule-based data quality checks for completeness, validity, and duplicates
- KPI outputs summarizing overall data quality health
- Breakdowns of quality issues by rule type and category
- Power BI dashboard export for governance and audit-style reporting

## Data
- Dataset includes intentional data quality issues to demonstrate validation logic
- Records are evaluated against defined quality rules
- Clean and failed records are separated to support transparency and remediation

## Key Outputs
- Overall data quality scorecard
- Counts and percentages of rule failures
- Identification of high-risk data quality areas
- Stakeholder-ready Power BI dashboard

## Dashboard
The Power BI dashboard presents data quality KPIs and rule failure distributions to support governance review and corrective action.

[View dashboard (PDF)](powerbi/data_quality_dashboard.pdf)

## Tools & Technologies
Python, SQL, Power BI, Excel

## Files
- Dashboard PDF: ./powerbi/
- Source code: ./src/
- Data outputs: ./data/

## Notes
This project is dashboard-first. The Power BI dashboard export is provided in the repository to demonstrate governance metrics and reporting structure.
