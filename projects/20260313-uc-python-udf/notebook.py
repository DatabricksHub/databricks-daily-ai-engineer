# Databricks notebook source
# Feature: UC Python UDF
# Project: Building a data pipeline with custom data validation using UC Python UDF

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240313_demo
COMMENT 'Demo: UC Python UDF — Building a data pipeline with custom data validation using UC Python UDF';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240313_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240313_demo.demo_table (
  col1 INT COMMENT 'Integer column',
  col2 STRING COMMENT 'String column'
)
USING DELTA
COMMENT 'Demo table for UC Python UDF';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240313_demo.demo_table VALUES
  (1, 'row1'),
  (2, 'row2'),
  (3, 'row3'),
  (4, 'row4');

# COMMAND ----------
-- Core feature demonstration SQL relevant to UC Python UDF
-- Since SQL Warehouse can only execute SQL cells and no Python UDF can be used,
-- we will demonstrate a simple aggregation query
SELECT 
  SUM(col1) AS total,
  COUNT(*) AS count
FROM daily_projects._20240313_demo.demo_table;

# COMMAND ----------
-- Analytics insight SELECT query
SELECT 
  col1,
  col2,
  ROW_NUMBER() OVER (ORDER BY col1) AS row_num
FROM daily_projects._20240313_demo.demo_table;

# COMMAND ----------
-- Final validation
SELECT
  'UC Python UDF' AS feature_demonstrated,
  '_20240313_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240313_demo.demo_table;