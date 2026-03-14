-- Query 1: Create a new table to store real-time sales data
/*
This query creates a new table called 'sales' in the 'real_time_analytics' database.
The table has columns for 'product_id', 'sale_date', 'quantity', and 'revenue'.
*/
CREATE TABLE IF NOT EXISTS real_time_analytics.sales (
  product_id INT,
  sale_date DATE,
  quantity INT,
  revenue DECIMAL(10, 2)
);

-- Query 2: Insert sample sales data into the 'sales' table
/*
This query inserts sample sales data into the 'sales' table.
The data includes 'product_id', 'sale_date', 'quantity', and 'revenue' for each sale.
*/
INSERT INTO real_time_analytics.sales (product_id, sale_date, quantity, revenue)
VALUES
  (1, '2022-01-01', 10, 100.00),
  (2, '2022-01-02', 20, 200.00),
  (3, '2022-01-03', 30, 300.00);

-- Query 3: Calculate daily sales revenue
/*
This query calculates the daily sales revenue by summing up the 'revenue' column
for each 'sale_date' in the 'sales' table.
The result is a table with 'sale_date' and 'daily_revenue' columns.
*/
SELECT 
  sale_date, 
  SUM(revenue) AS daily_revenue
FROM 
  real_time_analytics.sales
GROUP BY 
  sale_date;

-- Query 4: Calculate total sales revenue by product
/*
This query calculates the total sales revenue for each 'product_id' in the 'sales' table.
The result is a table with 'product_id' and 'total_revenue' columns.
*/
SELECT 
  product_id, 
  SUM(revenue) AS total_revenue
FROM 
  real_time_analytics.sales
GROUP BY 
  product_id;

-- Query 5: Calculate top 3 products by total sales revenue
/*
This query calculates the top 3 products by total sales revenue.
The result is a table with 'product_id' and 'total_revenue' columns, sorted in descending order by 'total_revenue'.
*/
SELECT 
  product_id, 
  SUM(revenue) AS total_revenue
FROM 
  real_time_analytics.sales
GROUP BY 
  product_id
ORDER BY 
  total_revenue DESC
LIMIT 3;