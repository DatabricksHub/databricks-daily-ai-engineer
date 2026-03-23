# Real-Time Mode in Apache Spark Structured Streaming Demo
## Overview
This demo project showcases the Real-Time Mode feature in Apache Spark Structured Streaming, enabling high-throughput and low-latency data processing. The project demonstrates a financial transaction monitoring system with real-time fraud detection, utilizing Apache Spark Structured Streaming, Delta Lake, and SQL window functions.

## Why this feature matters
Real-Time Mode in Apache Spark Structured Streaming is crucial for building scalable and efficient data pipelines that can handle large volumes of data in real-time. This enables businesses to make data-driven decisions quickly, making it ideal for real-time analytics and event-driven applications. In this demo, we apply this feature to a financial transaction monitoring system, allowing for timely detection of suspicious patterns and triggering alerts.

## Prerequisites
* Databricks Runtime with Apache Spark 3.3 or later
* Delta Lake enabled
* Basic understanding of Apache Spark, Structured Streaming, and SQL

## How to run
To run this demo, follow these steps:
1. **Import the notebook**: Import the provided Databricks notebook into your workspace.
2. **Create a Delta Lake table**: Run the SQL command to create a Delta Lake table for storing transaction history.
```sql
CREATE TABLE transactions (
  id INT,
  timestamp TIMESTAMP,
  amount DECIMAL(10, 2),
  user_id INT
) USING delta;
```
3. **Run the Structured Streaming query**: Execute the notebook to start the Structured Streaming query, which processes transactions and applies SQL window functions to identify suspicious patterns.
4. **Trigger alerts**: The query will trigger alerts when suspicious patterns are detected.

## Expected output
The demo will display the following output:
* A stream of processed transactions with detected suspicious patterns highlighted
* Alerts triggered in real-time when suspicious activity is detected

## Additional resources
For more information on the technologies used in this demo, refer to the following Databricks documentation:
* [Apache Spark Structured Streaming](https://docs.databricks.com/spark/latest/structured-streaming/index.html)
* [Delta Lake](https://docs.databricks.com/delta/index.html)
* [SQL Window Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions-window.html)
* [Real-Time Mode in Apache Spark Structured Streaming](https://docs.databricks.com/spark/latest/structured-streaming/real-time-mode.html)