```sql
-- Query 1: Daily Sales Aggregation
-- This query aggregates daily sales data from the 'sales' table, 
-- grouping by date and product, and calculates total sales and revenue.
SELECT 
  date, 
  product, 
  SUM(quantity) AS total_sales, 
  SUM(revenue) AS total_revenue
FROM 
  sales
GROUP BY 
  date, 
  product;

-- Query 2: Low-Stock Alerting
-- This query identifies products with low stock levels (less than 10 units) 
-- from the 'inventory' table, using a window function to rank products by stock level.
SELECT 
  product, 
  stock_level, 
  ROW_NUMBER() OVER (ORDER BY stock_level ASC) AS low_stock_rank
FROM 
  inventory
WHERE 
  stock_level < 10;

-- Query 3: SCD Type 2 History
-- This query retrieves the history of changes to product prices 
-- from the 'product_history' table, using a window function to track changes over time.
SELECT 
  product, 
  price, 
  effective_date, 
  expiration_date, 
  LAG(price) OVER (PARTITION BY product ORDER BY effective_date) AS previous_price
FROM 
  product_history
ORDER BY 
  product, 
  effective_date;

-- Query 4: Inventory Turnover Analysis
-- This query calculates inventory turnover for each product 
-- from the 'sales' and 'inventory' tables, using a join to combine data.
SELECT 
  i.product, 
  SUM(s.quantity) / i.stock_level AS inventory_turnover
FROM 
  sales s
JOIN 
  inventory i ON s.product = i.product
GROUP BY 
  i.product;

-- Query 5: Sales Trend Analysis
-- This query analyzes sales trends over time 
-- from the 'sales' table, using a window function to calculate moving averages.
SELECT 
  date, 
  product, 
  SUM(revenue) AS daily_revenue, 
  AVG(SUM(revenue)) OVER (PARTITION BY product ORDER BY date ROWS 6 PRECEDING) AS moving_average_revenue
FROM 
  sales
GROUP BY 
  date, 
  product
ORDER BY 
  date;
```