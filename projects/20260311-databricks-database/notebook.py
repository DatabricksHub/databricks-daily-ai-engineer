# Databricks notebook source
# Feature: Databricks Database
# Project: Building a real-time analytics dashboard using Databricks Database and SQL analytics

# COMMAND ----------
"""
This cell imports necessary libraries and sets up the configuration.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import *

# COMMAND ----------
"""
This cell creates or loads the required data and tables.
"""
# Create a schema for the data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create a sample DataFrame
data = spark.createDataFrame([
    (1, "John", 25),
    (2, "Alice", 30),
    (3, "Bob", 35)
], schema)

# Create a Delta table
data.write.format("delta").saveAsTable("default.people")

# COMMAND ----------
"""
This cell demonstrates the core feature of Databricks Database.
"""
# Create a new database
spark.sql("CREATE DATABASE IF NOT EXISTS mydatabase")

# Use the new database
spark.sql("USE mydatabase")

# Create a new table in the database
spark.sql("CREATE TABLE IF NOT EXISTS mytable (id INT, name STRING, age INT)")

# Insert data into the table
spark.sql("INSERT INTO mytable SELECT * FROM default.people")

# COMMAND ----------
"""
This cell performs analytics and transformations on the result.
"""
# Query the data
result = spark.sql("SELECT * FROM mydatabase.mytable WHERE age > 30")

# Print the result
result.show()

# COMMAND ----------
"""
This cell validates the result and prints a success message.
"""
if result.count() == 1:
    print("✅ Demo completed successfully")
else:
    print("❌ Demo failed")