# Databricks notebook source
# Feature: Delta Sharing
# Project: Securely sharing customer data between departments using Delta Sharing

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
# Create a sample customer data table
data = spark.createDataFrame([
    ("1", "John", "Doe", "john@example.com"),
    ("2", "Jane", "Doe", "jane@example.com"),
    ("3", "Bob", "Smith", "bob@example.com")
], ["id", "first_name", "last_name", "email"])

# Write the data to a Delta table
data.write.format("delta").saveAsTable("customer_data")

# COMMAND ----------
"""
Core feature demonstration code: Delta Sharing.
"""
# Create a Delta Sharing share
share_name = "customer_data_share"
dbutils.fs.rm(f"dbfs:/delta_sharing/{share_name}", True)
spark.sql(f"CREATE SHARE {share_name} WITH (type = 'delta_sharing')")
spark.sql(f"ALTER SHARE {share_name} ADD TABLE customer_data")

# COMMAND ----------
"""
Analytics / transformations on the result: Read the shared data.
"""
# Read the shared data
shared_data = spark.sql("SELECT * FROM customer_data")

# Apply some transformations to the shared data
transformed_data = shared_data.filter(col("first_name") == "John")

# COMMAND ----------
"""
Validation: Print a success message if the demo completes successfully.
"""
if transformed_data.count() == 1:
    print("✅ Demo completed successfully")
else:
    print("❌ Demo failed")