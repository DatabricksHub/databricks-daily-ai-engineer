# README.md: Delta Live Tables with MERGE Demo Project
## Overview
This demo project showcases the power of Delta Live Tables with MERGE in building real-time data pipelines for retail inventory management. The project demonstrates how to ingest data from multiple sources, transform and load it into Delta Lake, and generate reports and alerts for inventory management and sales analysis using SCD Type 2 tracking, daily sales aggregation, and low-stock alerting.

## Why this feature matters
Delta Live Tables with MERGE simplifies the process of building and managing data pipelines, making it easier to derive insights from data in real-time. This feature enables efficient data ingestion, transformation, and loading into Delta Lake, while providing a powerful way to upsert data and perform complex data integration and updates.

## Prerequisites
* Databricks Runtime 8.3 or later
* Delta Live Tables enabled on your Databricks workspace
* Basic understanding of SQL and data engineering concepts

## How to run
To run this demo project, follow these steps:
1. **Create a new Databricks notebook**: Create a new notebook in your Databricks workspace and attach it to a cluster with Databricks Runtime 8.3 or later.
2. **Clone the demo project**: Clone this demo project into your Databricks workspace.
3. **Run the notebook**: Run the notebook `retail_inventory_management` to ingest data from multiple sources, transform and load it into Delta Lake, and generate reports and alerts for inventory management and sales analysis.
4. **Execute SQL queries**: Execute the SQL queries in the `queries` folder to verify the results and explore the data.

## Expected output
The demo project will generate the following output:
* A Delta Live Table `inventory` with SCD Type 2 tracking
* A Delta Live Table `daily_sales` with daily sales aggregation
* A Delta Live Table `low_stock_alerts` with low-stock alerting
* Reports and alerts for inventory management and sales analysis

## Links to Databricks documentation
For more information on Delta Live Tables and MERGE, refer to the following Databricks documentation:
* [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/index.html)
* [MERGE](https://docs.databricks.com/delta/delta-batch.html#merge)
* [SCD Type 2](https://docs.databricks.com/data-engineering/delta-lake-tutorial/scd2.html)
* [Window functions](https://docs.databricks.com/sql/language-manual/functions/window-functions.html)