# Databricks notebook source
# Feature: Databricks Partner Connect
# Project: Integrating a third-party data source using Databricks Partner Connect for real-time analytics

# COMMAND ----------
"""
This cell imports the necessary libraries and configures the environment.
"""
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dbutils

# COMMAND ----------
"""
This cell creates or loads the required data and tables for the demonstration.
"""
# Create a sample DataFrame
data = spark.createDataFrame([
    ("John", 25, "USA"),
    ("Mary", 31, "Canada"),
    ("David", 42, "UK")
], ["Name", "Age", "Country"])

# Create a Delta table
data.write.format("delta").saveAsTable("default.people")

# COMMAND ----------
"""
This cell demonstrates the core feature of Databricks Partner Connect by integrating a third-party data source.
"""
# Define a function to simulate data from a third-party source
def get_third_party_data():
    # Simulate data from a third-party source
    data = spark.createDataFrame([
        ("USA", 100),
        ("Canada", 50),
        ("UK", 200)
    ], ["Country", "Population"])
    return data

# Get data from the third-party source
third_party_data = get_third_party_data()

# Join the third-party data with the existing Delta table
result = spark.table("default.people").join(third_party_data, on="Country")

# COMMAND ----------
"""
This cell performs analytics and transformations on the result.
"""
# Perform aggregations on the result
aggregated_result = result.groupBy("Country").agg(sum("Age").alias("Total Age"), sum("Population").alias("Total Population"))

# Sort the result in descending order by Total Population
sorted_result = aggregated_result.sort(col("Total Population").desc())

# COMMAND ----------
"""
This cell validates the result and prints a success message.
"""
# Validate the result
if sorted_result.count() > 0:
    print("✅ Demo completed successfully")
else:
    print("❌ Demo failed")