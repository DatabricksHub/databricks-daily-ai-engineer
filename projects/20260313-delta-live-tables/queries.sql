/*
  This query demonstrates the creation of a Delta Live Table pipeline, 
  which automates the ingestion and transformation of data from a source table.
*/
CREATE LIVE TABLE customer_data
COMMENT "Customer data pipeline"
TBLPROPERTIES ("pipelinedatabase" = "default")
AS 
SELECT 
  id, 
  name, 
  email, 
  phone, 
  address
FROM 
  bronze_customer_data;

/*
  This query showcases the automated data transformation and loading into a Delta Live Table, 
  using a pipeline to merge data from multiple sources.
*/
CREATE LIVE TABLE orders_data
COMMENT "Orders data pipeline with merge"
TBLPROPERTIES ("pipelinedatabase" = "default")
AS 
SELECT 
  o.id, 
  o.customer_id, 
  o.order_date, 
  c.name AS customer_name
FROM 
  bronze_orders o
JOIN 
  silver_customer_data c ON o.customer_id = c.id;

/*
  This query monitors the pipeline's progress and data quality, 
  providing insights into the data ingestion and transformation process.
*/
SELECT 
  pipeline_name, 
  table_name, 
  status, 
  last_updated
FROM 
  sys.pipeline_status
WHERE 
  pipeline_name = 'customer_data';

/*
  This query analyzes the transformed data in the Delta Live Table, 
  providing business insights and metrics on customer orders.
*/
SELECT 
  customer_name, 
  COUNT(*) AS order_count, 
  SUM(order_total) AS total_revenue
FROM 
  silver_orders_data
GROUP BY 
  customer_name
ORDER BY 
  total_revenue DESC;

/*
  This query demonstrates the use of Delta Live Tables for real-time data warehousing, 
  by creating a materialized view that provides up-to-date insights into customer behavior.
*/
CREATE MATERIALIZED VIEW customer_insights
COMMENT "Real-time customer insights"
TBLPROPERTIES ("delta.enableChangeDataFeed" = "true")
AS 
SELECT 
  c.id, 
  c.name, 
  COUNT(o.id) AS order_count, 
  SUM(o.order_total) AS total_spent
FROM 
  silver_customer_data c
JOIN 
  silver_orders_data o ON c.id = o.customer_id
GROUP BY 
  c.id, 
  c.name;