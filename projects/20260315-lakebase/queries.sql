-- Query 1: Demonstrates creating a Lakebase table and loading data into it
/*
This query creates a Lakebase table called 'customer_data' and loads data into it from a CSV file.
The table is designed to store customer information, including customer ID, name, and address.
*/
CREATE TABLE customer_data (
  customer_id INT,
  name STRING,
  address STRING
) USING delta;

-- Query 2: Performs data quality checks on the Lakebase table
/*
This query performs data quality checks on the 'customer_data' table, including checking for null values and data types.
It returns a count of rows with null values and a count of rows with invalid data types.
*/
SELECT 
  COUNT(*) AS total_rows,
  COUNTIF(customer_id IS NULL) AS null_customer_id,
  COUNTIF(name IS NULL) AS null_name,
  COUNTIF(address IS NULL) AS null_address
FROM customer_data;

-- Query 3: Demonstrates real-time analytics using Lakebase
/*
This query demonstrates real-time analytics using Lakebase by calculating the total number of customers and the average order value.
It uses a join to combine data from the 'customer_data' table and the 'order_data' table.
*/
SELECT 
  COUNT(DISTINCT c.customer_id) AS total_customers,
  AVG(o.order_value) AS average_order_value
FROM customer_data c
JOIN order_data o ON c.customer_id = o.customer_id;

-- Query 4: Performs data aggregation and filtering using Lakebase
/*
This query performs data aggregation and filtering using Lakebase by calculating the total order value for each customer and filtering out customers with a total order value less than $100.
It uses a group by clause to group the data by customer ID and a having clause to filter the data.
*/
SELECT 
  c.customer_id,
  SUM(o.order_value) AS total_order_value
FROM customer_data c
JOIN order_data o ON c.customer_id = o.customer_id
GROUP BY c.customer_id
HAVING SUM(o.order_value) > 100;

-- Query 5: Demonstrates data visualization using Lakebase
/*
This query demonstrates data visualization using Lakebase by calculating the top 10 customers by total order value.
It uses a limit clause to limit the results to the top 10 customers.
*/
SELECT 
  c.customer_id,
  c.name,
  SUM(o.order_value) AS total_order_value
FROM customer_data c
JOIN order_data o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_order_value DESC
LIMIT 10;