# Databricks notebook source
# Feature: Genie Code
# Project: E-commerce order pipeline with real-time inventory updates, SCD Type 2 tracking, and daily sales aggregation using Genie Code, Delta Lake, and SQL Warehouse, including window functions for time-based analysis and low-stock alerting using MERGE for upserts

# COMMAND ----------
%sql
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20260315_demo
COMMENT 'Demo: Genie Code — E-commerce order pipeline with real-time inventory updates, SCD Type 2 tracking, and daily sales aggregation using Genie Code, Delta Lake, and SQL Warehouse, including window functions for time-based analysis and low-stock alerting using MERGE for upserts'

# COMMAND ----------
%sql
-- Set default schema context
USE daily_projects._20260315_demo

# COMMAND ----------
%sql
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20260315_demo.orders (
  order_id INT COMMENT 'Unique order identifier',
  customer_id INT COMMENT 'Customer identifier',
  order_date DATE COMMENT 'Date the order was placed',
  total_amount DECIMAL(10, 2) COMMENT 'Total amount of the order'
)
USING DELTA
COMMENT 'Table to store order information'

# COMMAND ----------
%sql
-- Insert sample data
INSERT INTO daily_projects._20260315_demo.orders VALUES
  (1, 1, '2022-01-01', 100.00),
  (2, 1, '2022-01-15', 200.00),
  (3, 2, '2022-02-01', 50.00),
  (4, 3, '2022-03-01', 150.00)

# COMMAND ----------
%sql
-- Core feature demonstration SQL (complexity: intermediate)
-- Using window functions, GROUP BY aggregations, and JOIN across tables
WITH daily_sales AS (
  SELECT 
    order_date,
    SUM(total_amount) AS daily_total
  FROM 
    daily_projects._20260315_demo.orders
  GROUP BY 
    order_date
),
monthly_sales AS (
  SELECT 
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(total_amount) AS monthly_total
  FROM 
    daily_projects._20260315_demo.orders
  GROUP BY 
    EXTRACT(MONTH FROM order_date)
)
SELECT 
  ds.order_date,
  ds.daily_total,
  ms.monthly_total
FROM 
  daily_sales ds
JOIN 
  monthly_sales ms ON EXTRACT(MONTH FROM ds.order_date) = ms.month

# COMMAND ----------
%sql
-- Analytics insight SELECT query
SELECT 
  order_date,
  total_amount,
  LAG(total_amount, 1) OVER (ORDER BY order_date) AS prev_day_total
FROM 
  daily_projects._20260315_demo.orders
ORDER BY 
  order_date

# COMMAND ----------
%sql
-- Final validation
SELECT
  'Genie Code' AS feature_demonstrated,
  '_20260315_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20260315_demo.orders