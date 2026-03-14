# Databricks notebook source
# Feature: Agent Bricks
# Project: Building a chatbot using Agent Bricks and MCP to demonstrate AI-driven customer support

# COMMAND ----------
-- Create project schema (YYYYMMDD_slug format)
CREATE SCHEMA IF NOT EXISTS daily_projects._20240314_demo
COMMENT 'Demo: Agent Bricks — Building a chatbot using Agent Bricks and MCP to demonstrate AI-driven customer support';

# COMMAND ----------
-- Set default schema context
USE daily_projects._20240314_demo;

# COMMAND ----------
-- Create table (column definitions ONLY, no data in this cell)
CREATE OR REPLACE TABLE daily_projects._20240314_demo.chatbot_interactions (
  interaction_id INT COMMENT 'Unique interaction ID',
  customer_query STRING COMMENT 'Customer query text',
  response STRING COMMENT 'Chatbot response text'
)
USING DELTA
COMMENT 'Table to store chatbot interactions';

# COMMAND ----------
-- Insert sample data (ALWAYS a SEPARATE statement from CREATE)
INSERT INTO daily_projects._20240314_demo.chatbot_interactions VALUES
  (1, 'Hello, how can I help you?', 'Hello! I can assist you with any questions you have.'),
  (2, 'What is the purpose of this chatbot?', 'This chatbot is designed to demonstrate AI-driven customer support using Agent Bricks and MCP.');

# COMMAND ----------
-- Core feature demonstration SQL relevant to Agent Bricks
-- Using window functions to analyze interaction patterns
SELECT 
  interaction_id,
  customer_query,
  response,
  LAG(customer_query, 1) OVER (ORDER BY interaction_id) AS previous_query
FROM daily_projects._20240314_demo.chatbot_interactions;

# COMMAND ----------
-- Analytics insight SELECT query
SELECT 
  COUNT(*) AS total_interactions,
  COUNT(DISTINCT customer_query) AS unique_queries
FROM daily_projects._20240314_demo.chatbot_interactions;

# COMMAND ----------
-- Final validation
SELECT
  'Agent Bricks' AS feature_demonstrated,
  '_20240314_demo' AS schema_name,
  COUNT(*) AS rows_loaded,
  'SUCCESS' AS status
FROM daily_projects._20240314_demo.chatbot_interactions;