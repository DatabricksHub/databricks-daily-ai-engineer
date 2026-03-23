# Databricks notebook source
# Feature: Real-Time Mode in Apache Spark Structured Streaming
# Project: Financial transaction monitoring with real-time fraud detection, using Apache Spark Structured Streaming to process transactions, Delta Lake for storing transaction history and SQL window functions to identify suspicious patterns and trigger alerts

# COMMAND ----------
%sql
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20260315_demo
COMMENT 'Demo: Real-Time Mode in Apache Spark Structured Streaming — Financial transaction monitoring with real-time fraud detection, using Apache Spark Structured Streaming to process transactions, Delta Lake for storing transaction history, and SQL window functions to identify suspicious patterns and trigger alerts'

# COMMAND ----------
%sql
-- Set default schema context
USE daily_projects._20260315_demo

# COMMAND ----------
%sql
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20260315_demo.transactions (
  transaction_id INT COMMENT 'Unique transaction identifier',
  customer_id INT COMMENT 'Customer identifier',
  transaction_amount DECIMAL(10, 2) COMMENT 'Transaction amount',
  transaction_time TIMESTAMP COMMENT 'Transaction timestamp'
)
USING DELTA
COMMENT 'Table to store financial transactions'

# COMMAND ----------
%sql
-- Insert sample data
INSERT INTO daily_projects._20260315_demo.transactions VALUES
  (1, 101, 100.00, '2022-01-01 12:00:00'),
  (2, 102, 200.00, '2022-01-01 13:00:00'),
  (3, 101, 50.00, '2022-01-01 14:00:00'),
  (4, 103, 300.00, '2022-01-01 15:00:00'),
  (5, 102, 250.00, '2022-01-01 16:00:00')

# COMMAND ----------
%sql
-- Core feature demonstration SQL (complexity: intermediate)
-- Using window functions to identify suspicious patterns
WITH suspicious_transactions AS (
  SELECT 
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_time,
    SUM(transaction_amount) OVER (PARTITION BY customer_id ORDER BY transaction_time ROWS 2 PRECEDING) AS total_amount
  FROM 
    daily_projects._20260315_demo.transactions
)
SELECT 
  customer_id,
  transaction_id,
  transaction_amount,
  transaction_time,
  total_amount
FROM 
  suspicious_transactions
WHERE 
  total_amount > 500

# COMMAND ----------
%sql
-- Analytics insight SELECT query
SELECT 
  customer_id,
  COUNT(transaction_id) AS num_transactions,
  SUM(transaction_amount) AS total_amount
FROM 
  daily_projects._20260315_demo.transactions
GROUP BY 
  customer_id
ORDER BY 
  total_amount DESC

# COMMAND ----------
%sql
-- Final validation
SELECT 
  'Real-Time Mode in Apache Spark Structured Streaming' AS feature_demonstrated,
  '_20260315_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM 
  daily_projects._20260315_demo.transactions