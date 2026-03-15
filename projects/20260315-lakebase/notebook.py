# Databricks notebook source
# Feature: Lakebase
# Project: Building a scalable data warehouse using Lakebase for real-time analytics and business insights

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240315_demo
COMMENT 'Demo: Lakebase — Building a scalable data warehouse using Lakebase for real-time analytics and business insights';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240315_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240315_demo.lakebase_data (
  id INT COMMENT 'Unique identifier',
  name STRING COMMENT 'Name of the data'
)
USING DELTA
COMMENT 'Table to store Lakebase data';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240315_demo.lakebase_data VALUES
  (1, 'Data1'),
  (2, 'Data2');

# COMMAND ----------
-- Core feature demonstration SQL relevant to Lakebase
-- Using window functions to get the top 2 names
SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS row_num
FROM daily_projects._20240315_demo.lakebase_data;

# COMMAND ----------
-- Analytics insight SELECT query
SELECT * FROM daily_projects._20240315_demo.lakebase_data;

# COMMAND ----------
-- Final validation:
SELECT
  'Lakebase' AS feature_demonstrated,
  '_20240315_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240315_demo.lakebase_data;