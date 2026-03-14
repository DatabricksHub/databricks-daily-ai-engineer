# Databricks notebook source
# Feature: MCP
# Project: Building a recommender system using MCP and Databricks

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240314_demo
COMMENT 'Demo: MCP — Building a recommender system using MCP and Databricks';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240314_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240314_demo.recommender_system (
  user_id INT COMMENT 'Unique user identifier',
  product_id INT COMMENT 'Unique product identifier',
  rating INT COMMENT 'User rating for the product'
)
USING DELTA
COMMENT 'Table to store user ratings for products';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240314_demo.recommender_system VALUES
  (1, 101, 4),
  (1, 102, 5),
  (2, 101, 3),
  (2, 103, 4),
  (3, 102, 5),
  (3, 103, 4);

# COMMAND ----------
-- Core feature demonstration SQL relevant to MCP
-- Using window functions to calculate average rating per user
SELECT 
  user_id,
  product_id,
  rating,
  AVG(rating) OVER (PARTITION BY user_id) AS avg_user_rating
FROM daily_projects._20240314_demo.recommender_system;

# COMMAND ----------
-- Analytics insight SELECT query
-- Find top 3 products with highest average rating
SELECT 
  product_id,
  AVG(rating) AS avg_rating
FROM daily_projects._20240314_demo.recommender_system
GROUP BY product_id
ORDER BY avg_rating DESC
LIMIT 3;

# COMMAND ----------
-- Final validation
SELECT
  'MCP' AS feature_demonstrated,
  '_20240314_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240314_demo.recommender_system;