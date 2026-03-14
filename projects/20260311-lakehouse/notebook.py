# Databricks notebook source
# Feature: Lakehouse
# Project: Building a real-time analytics dashboard using Lakehouse for sales data

# COMMAND ----------
"""
Import necessary libraries and configure the Spark session.
"""
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql import functions as F
from delta.tables import *

# COMMAND ----------
"""
Create or load required data/tables.
"""
# Create a sample sales data DataFrame
data = spark.createDataFrame([
    ("2022-01-01", "Product A", 100),
    ("2022-01-02", "Product B", 200),
    ("2022-01-03", "Product A", 150),
    ("2022-01-04", "Product B", 250),
    ("2022-01-05", "Product A", 120),
    ("2022-01-06", "Product B", 220)
], ["date", "product", "sales"])

# Write the DataFrame to a Delta table
data.write.format("delta").saveAsTable("sales_data")

# COMMAND ----------
"""
Core feature demonstration code: 
Create a Lakehouse by combining the best of data warehouses and data lakes.
"""
# Create a Delta table for the sales data
delta_table = DeltaTable.forPath(spark, "sales_data")

# Create a view on top of the Delta table for data warehousing
spark.sql("CREATE OR REPLACE VIEW sales_view AS SELECT * FROM sales_data")

# COMMAND ----------
"""
Analytics/transformations on the result: 
Perform real-time analytics on the sales data using the Lakehouse.
"""
# Query the sales view to get the total sales for each product
total_sales = spark.sql("SELECT product, SUM(sales) AS total_sales FROM sales_view GROUP BY product")

# Display the results
total_sales.show()

# COMMAND ----------
"""
Validation: 
Print a success message if the demo is completed successfully.
"""
print("✅ Demo completed successfully")