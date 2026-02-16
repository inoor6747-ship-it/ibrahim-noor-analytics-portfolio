# Business Process Performance Analyzer

## Purpose
Operations teams often know tickets are slow, but not *where* delays happen.  
This project analyzes a ticket workflow to identify SLA risk, bottleneck stages, and ownership gaps.

## What This Project Shows
- Workflow lifecycle analysis
- Data validation and cleaning
- SLA performance measurement
- Operational dashboard reporting

## Data
Synthetic ticket lifecycle dataset (5k+ records) with intentional issues:
- Duplicate tickets
- Missing timestamps
- Invalid event sequences

The pipeline separates raw, clean, and rejected records before analysis.

## Analysis Performed
- Cycle time calculation by workflow stage
- SLA breach rate by priority and owner team
- Bottleneck detection
- Team performance comparison

## Key Findings
- One workflow stage created the majority of delays
- Critical priority tickets had the highest breach rates
- SLA performance varied significantly by owner team

## Dashboard
[View Dashboard](powerbi/business_process_analyzer_dashboard.pdf)

## Tech Used
Python (pandas), SQL, Power BI, Excel

## Why It Matters
Demonstrates how operational data can be transformed into clear process improvement decisions.
