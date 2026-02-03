# Business Process Performance Analyzer

## Business Problem
Operations teams often lack visibility into where delays occur across an end-to-end workflow, making it difficult to identify SLA risks, process bottlenecks, and ownership gaps. This project analyzes ticket lifecycle data to measure cycle time, SLA breach rates, and bottleneck stages to support data-driven process improvements.

## Data
- Generated realistic ticket lifecycle data with intentional data quality issues (duplicates, missing timestamps, invalid timelines)
- Separated raw, clean, and rejected records to demonstrate data validation practices
- Computed KPI summary tables for executive reporting

## Approach
- Built a Python pipeline to generate, clean, and validate workflow data
- Calculated cycle time metrics and SLA breach indicators
- Aggregated results by priority, category, owner team, and bottleneck stage
- Visualized KPIs and trends in Power BI for stakeholder-ready reporting

## Key Insights
- Critical-priority tickets account for the highest SLA breach rates
- Cycle times are relatively consistent across categories, indicating systemic process delays rather than category-specific issues
- One workflow stage accounts for the majority of bottlenecks, highlighting a clear improvement opportunity
- SLA performance varies by owner team, suggesting targeted process or staffing interventions

## Dashboard Preview
![Dashboard](powerbi/dashboard.png)

## Tools & Technologies
Python (pandas, numpy), SQL, Power BI, Excel

## How to Run
```bash
pip install pandas numpy faker
python src/generate_data.py
python src/analyze.py
