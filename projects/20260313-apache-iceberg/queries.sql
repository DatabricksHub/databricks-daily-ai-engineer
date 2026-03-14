```sql
-- Query 1: Create an Apache Iceberg table to store customer data
/*
  This query creates an Iceberg table named 'customers' with columns for customer ID, name, and email.
  The table is created in the 'default' database and uses the 'iceberg' format.
*/
CREATE TABLE default.customers (
  customer_id INT,
  name STRING,
  email STRING
) USING iceberg;

-- Query 2: Insert sample data into the 'customers' table
/*
  This query inserts sample data into the 'customers' table.
  The data includes customer IDs, names, and email addresses.
*/
INSERT INTO default.customers (customer_id, name, email)
VALUES
  (1, 'John Doe', 'john.doe@example.com'),
  (2, 'Jane Doe', 'jane.doe@example.com'),
  (3, 'Bob Smith', 'bob.smith@example.com');

-- Query 3: Create an Iceberg table to store order data with a foreign key reference to 'customers'
/*
  This query creates an Iceberg table named 'orders' with columns for order ID, customer ID, order date, and total cost.
  The table has a foreign key constraint referencing the 'customer_id' column in the 'customers' table.
*/
CREATE TABLE default.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  total_cost DECIMAL(10, 2),
  CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES default.customers (customer_id)
) USING iceberg;

-- Query 4: Insert sample data into the 'orders' table
/*
  This query inserts sample data into the 'orders' table.
  The data includes order IDs, customer IDs, order dates, and total costs.
*/
INSERT INTO default.orders (order_id, customer_id, order_date, total_cost)
VALUES
  (1, 1, '2022-01-01', 100.00),
  (2, 1, '2022-01-15', 200.00),
  (3, 2, '2022-02-01', 50.00);

-- Query 5: Run a query to analyze customer order history and total spend
/*
  This query joins the 'customers' and 'orders' tables to analyze customer order history and total spend.
  The results include the customer name, total number of orders, and total spend.
*/
SELECT 
  c.name, 
  COUNT(o.order_id) AS total_orders, 
  SUM(o.total_cost) AS total_spend
FROM 
  default.customers c
  JOIN default.orders o ON c.customer_id = o.customer_id
GROUP BY 
  c.name
ORDER BY 
  total_spend DESC;
```