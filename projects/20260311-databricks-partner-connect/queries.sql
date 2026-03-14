```sql
-- Query 1: Demonstrating real-time data ingestion from a third-party source
/*
  This query showcases the ability to ingest data in real-time from a third-party source 
  using Databricks Partner Connect. It selects the most recent data from the 'customer_data' 
  table, which is assumed to be created in the companion notebook.
*/
SELECT * FROM customer_data ORDER BY timestamp DESC LIMIT 10;

-- Query 2: Analyzing customer behavior based on real-time data
/*
  This query analyzes customer behavior by calculating the average order value 
  for each customer in the last hour. It joins the 'customer_data' table with the 
  'orders' table, also assumed to be created in the companion notebook.
*/
SELECT cd.customer_id, AVG(o.order_value) AS avg_order_value
FROM customer_data cd
JOIN orders o ON cd.customer_id = o.customer_id
WHERE o.timestamp > CURRENT_TIMESTAMP - INTERVAL 1 hour
GROUP BY cd.customer_id;

-- Query 3: Creating a real-time dashboard for customer insights
/*
  This query creates a real-time dashboard for customer insights by calculating 
  key metrics such as customer count, average order value, and total revenue. 
  It uses the 'customer_data' and 'orders' tables created in the companion notebook.
*/
SELECT 
  COUNT(DISTINCT cd.customer_id) AS customer_count,
  AVG(o.order_value) AS avg_order_value,
  SUM(o.order_value) AS total_revenue
FROM customer_data cd
JOIN orders o ON cd.customer_id = o.customer_id
WHERE o.timestamp > CURRENT_TIMESTAMP - INTERVAL 1 day;

-- Query 4: Identifying top-spending customers in real-time
/*
  This query identifies the top-spending customers in real-time by calculating 
  the total order value for each customer in the last hour. It uses the 'customer_data' 
  and 'orders' tables created in the companion notebook.
*/
SELECT cd.customer_id, SUM(o.order_value) AS total_order_value
FROM customer_data cd
JOIN orders o ON cd.customer_id = o.customer_id
WHERE o.timestamp > CURRENT_TIMESTAMP - INTERVAL 1 hour
GROUP BY cd.customer_id
ORDER BY total_order_value DESC LIMIT 10;

-- Query 5: Monitoring data ingestion latency in real-time
/*
  This query monitors the data ingestion latency in real-time by calculating 
  the time difference between the data ingestion timestamp and the current timestamp. 
  It uses the 'customer_data' table created in the companion notebook.
*/
SELECT 
  MAX(timestamp) AS latest_ingestion_time,
  CURRENT_TIMESTAMP - MAX(timestamp) AS ingestion_latency
FROM customer_data;
```