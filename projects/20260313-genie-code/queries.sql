```sql
-- Query 1: Data Quality Check
/*
  This query checks the data quality of the orders table by counting the number of rows, 
  null values in each column, and the data types of each column.
*/
SELECT 
  COUNT(*) AS total_rows,
  SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_order_date
FROM 
  orders;

-- Query 2: Top 10 Products by Revenue
/*
  This query calculates the top 10 products by revenue by joining the orders and order_items tables, 
  grouping by product_id, and sorting by total revenue in descending order.
*/
SELECT 
  p.product_id,
  p.product_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM 
  orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN products p ON oi.product_id = p.product_id
GROUP BY 
  p.product_id, p.product_name
ORDER BY 
  total_revenue DESC
LIMIT 10;

-- Query 3: Customer Segmentation
/*
  This query segments customers based on their total spend by joining the orders and order_items tables, 
  grouping by customer_id, and sorting by total spend in descending order.
*/
SELECT 
  c.customer_id,
  c.customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_spend
FROM 
  orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN customers c ON o.customer_id = c.customer_id
GROUP BY 
  c.customer_id, c.customer_name
ORDER BY 
  total_spend DESC;

-- Query 4: Order Distribution by Date
/*
  This query calculates the distribution of orders by date by grouping the orders table by order_date, 
  and sorting by order_date in ascending order.
*/
SELECT 
  o.order_date,
  COUNT(*) AS num_orders
FROM 
  orders o
GROUP BY 
  o.order_date
ORDER BY 
  o.order_date ASC;

-- Query 5: Product Category Performance
/*
  This query calculates the performance of each product category by joining the orders, order_items, 
  and products tables, grouping by category, and sorting by total revenue in descending order.
*/
SELECT 
  p.category,
  SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM 
  orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  JOIN products p ON oi.product_id = p.product_id
GROUP BY 
  p.category
ORDER BY 
  total_revenue DESC;
```