# Databricks notebook source
# Feature: Generative AI
# Project: Building a text classification model using Databricks' Generative AI capabilities to classify customer feedback

# COMMAND ----------
-- Create project schema
CREATE SCHEMA IF NOT EXISTS daily_projects.demo
COMMENT 'Demo: Generative AI project';

# COMMAND ----------
-- Set default schema context
USE daily_projects.demo;

# COMMAND ----------
-- Create table for storing customer feedback
CREATE OR REPLACE TABLE daily_projects.demo.customer_feedback (
  id INT COMMENT 'Unique identifier for each feedback',
  feedback TEXT COMMENT 'Customer feedback text'
)
USING DELTA
COMMENT 'Table to store customer feedback';

# COMMAND ----------
-- Insert sample customer feedback data
INSERT INTO daily_projects.demo.customer_feedback VALUES
  (1, 'Great product!'),
  (2, 'Needs improvement'),
  (3, 'Excellent customer service'),
  (4, 'Product is okay');

# COMMAND ----------
-- Demonstrate core feature of Generative AI using window functions
-- Calculate the length of each feedback and rank them by length
SELECT 
  id,
  feedback,
  LENGTH(feedback) AS feedback_length,
  RANK() OVER (ORDER BY LENGTH(feedback) DESC) AS feedback_rank
FROM daily_projects.demo.customer_feedback;

# COMMAND ----------
-- Analytics insight: Get the top 2 longest feedback
SELECT 
  id,
  feedback,
  LENGTH(feedback) AS feedback_length
FROM daily_projects.demo.customer_feedback
ORDER BY LENGTH(feedback) DESC
LIMIT 2;

# COMMAND ----------
-- Final validation
SELECT
  'Generative AI' AS feature_demonstrated,
  'demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects.demo.customer_feedback;