# Databricks notebook source
# Feature: Databricks Marketplace
# Project: Building a data pipeline using Databricks Marketplace to integrate with external data sources and create a unified data analytics platform

# COMMAND ----------
"""
Import necessary libraries and configure the Spark session.
"""
from pyspark.sql.functions import col
from delta.tables import *

# COMMAND ----------
"""
Create or load required data / tables.
"""
# Create a sample Delta table
data = spark.createDataFrame([
    ("John", 25, "USA"),
    ("Mary", 31, "Canada"),
    ("David", 42, "UK")
], ["Name", "Age", "Country"])

data.write.format("delta").saveAsTable("sample_data")

# COMMAND ----------
"""
Core feature demonstration code: 
Integrate with external data sources using Databricks Marketplace.
For demonstration purposes, we will simulate this by creating another Delta table.
"""
# Create another sample Delta table to simulate external data
external_data = spark.createDataFrame([
    ("USA", "North America"),
    ("Canada", "North America"),
    ("UK", "Europe")
], ["Country", "Region"])

external_data.write.format("delta").saveAsTable("external_data")

# COMMAND ----------
"""
Analytics / transformations on the result: 
Join the sample data with the external data and perform some analytics.
"""
# Load the Delta tables
sample_data = spark.table("sample_data")
external_data = spark.table("external_data")

# Join the tables and perform some analytics
result = sample_data.join(external_data, sample_data.Country == external_data.Country, "inner") \
    .groupBy("Region") \
    .count()

result.show()

# COMMAND ----------
"""
Validation: 
Print a success message if the demo is completed successfully.
"""
print("✅ Demo completed successfully")