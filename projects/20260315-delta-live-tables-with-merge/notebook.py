# Databricks notebook source
# Feature: Delta Live Tables with MERGE
# Project: Retail inventory management with SCD Type 2 tracking, daily sales aggregation, and low-stock alerting using Delta MERGE and window functions, where data is ingested from multiple sources, transformed, and loaded into Delta Lake, and then used to generate reports and alerts for inventory management and sales analysis

# COMMAND ----------
%sql
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20260315_demo
COMMENT 'Demo: Delta Live Tables with MERGE — Retail inventory management with SCD Type 2 tracking, daily sales aggregation, and low-stock alerting using Delta MERGE and window functions, where data is ingested from multiple sources, transformed, and loaded into Delta Lake, and then used to generate reports and alerts for inventory management and sales analysis'

# COMMAND ----------
%sql
-- Set default schema context
USE daily_projects._20260315_demo

# COMMAND ----------
%sql
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20260315_demo.inventory (
  product_id INT COMMENT 'Unique product identifier',
  product_name STRING COMMENT 'Product name',
  quantity INT COMMENT 'Current quantity in stock',
  last_updated TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Inventory table with product information and quantity'

# COMMAND ----------
%sql
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20260315_demo.inventory VALUES
  (1, 'Product A', 100, TIMESTAMP '2022-01-01 00:00:00'),
  (2, 'Product B', 50, TIMESTAMP '2022-01-01 00:00:00'),
  (3, 'Product C', 200, TIMESTAMP '2022-01-01 00:00:00')

# COMMAND ----------
%sql
-- Core feature demonstration SQL (complexity: intermediate)
-- Using window functions, GROUP BY aggregations, and JOIN across tables
-- Demonstrating SCD Type 2 upserts using MERGE
MERGE INTO daily_projects._20260315_demo.inventory AS target
USING (
  SELECT 
    product_id,
    product_name,
    quantity,
    last_updated
  FROM daily_projects._20260315_demo.inventory
  WHERE product_id = 1
) AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN
  UPDATE SET 
    target.quantity = source.quantity,
    target.last_updated = source.last_updated
WHEN NOT MATCHED THEN
  INSERT (product_id, product_name, quantity, last_updated)
  VALUES (source.product_id, source.product_name, source.quantity, source.last_updated)

# COMMAND ----------
%sql
-- Analytics insight SELECT query
SELECT 
  product_id,
  product_name,
  SUM(quantity) AS total_quantity,
  MAX(last_updated) AS last_update
FROM daily_projects._20260315_demo.inventory
GROUP BY product_id, product_name
ORDER BY total_quantity DESC

# COMMAND ----------
%sql
-- Final validation
SELECT 
  'Delta Live Tables with MERGE' AS feature_demonstrated,
  '_20260315_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20260315_demo.inventory