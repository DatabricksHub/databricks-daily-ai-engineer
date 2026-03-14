# README.md: Real-time Data Warehousing with Delta Live Tables
## Overview
This demo project showcases the capabilities of Delta Live Tables, a feature in Databricks that enables users to build and manage data pipelines with a simple, declarative syntax. The project demonstrates real-time data warehousing using Delta Live Tables for automated data ingestion and transformation.

## Why this feature matters
Delta Live Tables simplifies the process of building and managing data pipelines, making it easier to streamline data processing and analytics. With Delta Live Tables, users can:
* Define data pipelines using a simple, declarative syntax
* Automate data ingestion and transformation
* Ensure data quality and reliability
* Scale data pipelines with ease

This feature is particularly useful for organizations that require real-time data warehousing and analytics, as it enables them to make data-driven decisions quickly and efficiently.

## Prerequisites
To run this demo project, you will need:
* A Databricks workspace with a cluster configured
* The `dlt` library installed in your Databricks workspace
* A sample dataset (provided in the demo project)

## How to run
To run the demo project, follow these steps:
1. **Create a new notebook**: Create a new notebook in your Databricks workspace and name it `Delta Live Tables Demo`.
2. **Import the DLT library**: Import the `dlt` library in your notebook using the following command: `%python import dlt`
3. **Define the pipeline**: Define the data pipeline using the `dlt` library. You can use the following SQL code as an example:
```sql
-- Create a new DLT pipeline
CREATE LIVE TABLE customer_data
COMMENT "Customer data pipeline"
TBLPROPERTIES ("quality" = "high")
AS
SELECT * FROM customer_source;

-- Create a new DLT pipeline with data transformation
CREATE LIVE TABLE customer_transformed
COMMENT "Transformed customer data pipeline"
TBLPROPERTIES ("quality" = "high")
AS
SELECT *, upper(name) AS name_upper FROM customer_data;
```
4. **Start the pipeline**: Start the pipeline using the following command: `dlt apply`
5. **Monitor the pipeline**: Monitor the pipeline using the Databricks UI or the `dlt` library.

## Expected output
The expected output of the demo project is a real-time data warehouse with automated data ingestion and transformation. You should see the following:
* A `customer_data` table with raw customer data
* A `customer_transformed` table with transformed customer data
* A pipeline that automatically ingests and transforms data in real-time

## Links to Databricks documentation
For more information on Delta Live Tables and Databricks, please refer to the following documentation:
* [Delta Live Tables documentation](https://docs.databricks.com/workflows/delta-live-tables/index.html)
* [Databricks documentation](https://docs.databricks.com/)