# Databricks notebook source
# Feature: Apache Iceberg
# Project: Building a scalable data warehouse using Apache Iceberg and Databricks for real-time analytics

# COMMAND ----------
"""
Import necessary libraries and configure the Spark session.
"""
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta.tables import *

# COMMAND ----------
"""
Create or load required data / tables.
"""
data = spark.createDataFrame([
    ("John", 25, "USA"),
    ("Linda", 22, "UK"),
    ("David", 35, "Australia")
], ["Name", "Age", "Country"])

data.write.format("iceberg").mode("overwrite").saveAsTable("iceberg_table")

# COMMAND ----------
"""
Core feature demonstration code: 
Create an Iceberg table and perform basic operations.
"""
iceberg_table = spark.table("iceberg_table")
print("Iceberg Table Schema:")
iceberg_table.printSchema()

# COMMAND ----------
"""
Analytics / transformations on the result: 
Perform aggregations and filtering on the Iceberg table.
"""
result = iceberg_table.groupBy("Country").count()
result = result.filter(result["count"] > 0)
print("Aggregated Result:")
result.show()

# COMMAND ----------
"""
Validation: 
Print a success message if the demo completes without errors.
"""
print("Demo completed successfully")