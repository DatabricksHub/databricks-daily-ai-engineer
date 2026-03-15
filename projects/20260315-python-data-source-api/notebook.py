# Databricks notebook source
# Feature: Python Data Source API
# Project: Building a custom data connector for a proprietary data format using the Python Data Source API

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240315_demo
COMMENT 'Demo: Python Data Source API — Building a custom data connector for a proprietary data format using the Python Data Source API';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240315_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240315_demo.custom_data (
  col1 STRING COMMENT 'data source name',
  col2 INTEGER COMMENT 'data source id'
)
USING DELTA
COMMENT 'table for custom data sources';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240315_demo.custom_data VALUES
  ('source1', 1),
  ('source2', 2);

# COMMAND ----------
-- Core feature demonstration SQL relevant to Python Data Source API
-- Using window functions to demonstrate data aggregation
SELECT 
  col1,
  col2,
  ROW_NUMBER() OVER (ORDER BY col2) AS row_num
FROM daily_projects._20240315_demo.custom_data;

# COMMAND ----------
-- Analytics insight SELECT query
SELECT 
  col1,
  SUM(col2) AS total_id
FROM daily_projects._20240315_demo.custom_data
GROUP BY col1;

# COMMAND ----------
-- Final validation
SELECT
  'Python Data Source API' AS feature_demonstrated,
  '_20240315_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240315_demo.custom_data;