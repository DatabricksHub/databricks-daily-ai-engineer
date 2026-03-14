```sql
-- Query 1: Daily Sales Summary
-- This query calculates the total sales amount and number of orders for each day
-- It demonstrates the ability to perform aggregations on large datasets in the Lakehouse
SELECT 
  date(order_date) AS order_date,
  SUM(sales_amount) AS total_sales,
  COUNT(order_id) AS num_orders
FROM 
  sales_data
GROUP BY 
  date(order_date)
ORDER BY 
  order_date DESC;

-- Query 2: Top-Performing Products
-- This query identifies the top 10 products by total sales amount
-- It showcases the Lakehouse's ability to handle complex queries and rankings
SELECT 
  product_id,
  product_name,
  SUM(sales_amount) AS total_sales
FROM 
  sales_data
  JOIN products ON sales_data.product_id = products.product_id
GROUP BY 
  product_id, product_name
ORDER BY 
  total_sales DESC
LIMIT 10;

-- Query 3: Regional Sales Distribution
-- This query calculates the total sales amount for each region
-- It demonstrates the Lakehouse's ability to handle geospatial data and aggregations
SELECT 
  region,
  SUM(sales_amount) AS total_sales
FROM 
  sales_data
  JOIN customers ON sales_data.customer_id = customers.customer_id
GROUP BY 
  region
ORDER BY 
  total_sales DESC;

-- Query 4: Sales Trend Over Time
-- This query calculates the total sales amount for each month over the past year
-- It showcases the Lakehouse's ability to handle time-series data and aggregations
SELECT 
  date_trunc('month', order_date) AS order_month,
  SUM(sales_amount) AS total_sales
FROM 
  sales_data
WHERE 
  order_date >= date_add(current_date, -365)
GROUP BY 
  order_month
ORDER BY 
  order_month ASC;

-- Query 5: Customer Segmentation
-- This query identifies the top 10 customers by total sales amount
-- It demonstrates the Lakehouse's ability to handle customer data and aggregations
SELECT 
  customer_id,
  customer_name,
  SUM(sales_amount) AS total_sales
FROM 
  sales_data
  JOIN customers ON sales_data.customer_id = customers.customer_id
GROUP BY 
  customer_id, customer_name
ORDER BY 
  total_sales DESC
LIMIT 10;
```