# customer-activity-pipeline
# Customer Activity Data Pipeline

## Overview
This project demonstrates a data engineering pipeline using PySpark and Spark SQL to process customer activity logs.

## Tools Used
- Databricks
- PySpark
- Spark SQL
- Parquet

## Pipeline Steps
1. Ingest raw activity data
2. Clean invalid records (nulls, negative values)
3. Transform data using Spark SQL
4. Generate business metrics:
   - Total watch time per customer
   - Top content by engagement
   - Daily active users

## Key Features
- Data validation and cleaning
- Aggregation and analytics using Spark SQL
- End-to-end pipeline simulation

## How to Run
Run the notebook in Databricks

## Business Use Case
Used to analyze customer engagement and content performance in a media platform.
