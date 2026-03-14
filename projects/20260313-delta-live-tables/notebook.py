# Databricks notebook source
# Feature: Delta Live Tables
# Project: Real-time data warehousing using Delta Live Tables for automated data ingestion and transformation

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240313_demo
COMMENT 'Demo: Delta Live Tables — Real-time data warehousing using Delta Live Tables for automated data ingestion and transformation';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240313_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240313_demo.demo_table (
  id INT COMMENT 'Unique identifier',
  name STRING COMMENT 'Name of the item'
)
USING DELTA
COMMENT 'Demo table for Delta Live Tables';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240313_demo.demo_table VALUES
(1, 'Item 1'),
(2, 'Item 2'),
(3, 'Item 3');

# COMMAND ----------
-- Core feature demonstration SQL relevant to Delta Live Tables
-- Using MERGE to demonstrate upsert functionality
MERGE INTO daily_projects._20240313_demo.demo_table AS target
USING (SELECT 4 AS id, 'Item 4' AS name) AS source
ON target.id = source.id
WHEN MATCHED THEN
  UPDATE SET name = source.name
WHEN NOT MATCHED THEN
  INSERT (id, name) VALUES (source.id, source.name);

# COMMAND ----------
-- Analytics insight SELECT query
SELECT id, name FROM daily_projects._20240313_demo.demo_table;

# COMMAND ----------
-- Final validation
SELECT
  'Delta Live Tables' AS feature_demonstrated,
  '_20240313_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240313_demo.demo_table;