```sql
-- Query 1: Create a Delta Share for customer data
/*
  This query creates a Delta Share named 'customer_data_share' 
  that includes the 'customer_info' and 'customer_purchases' tables.
  The share is created with the 'reader' permission, allowing recipients 
  to read the data but not modify it.
*/
CREATE SHARE customer_data_share
COMMENT 'Share for customer data'
WITH RECIPIENT 'recipient_account' AS
SELECT * FROM customer_info
UNION ALL
SELECT * FROM customer_purchases;

-- Query 2: List all Delta Shares
/*
  This query lists all Delta Shares that are currently available, 
  including the 'customer_data_share' created in the previous query.
*/
SHOW SHARES;

-- Query 3: Get the schema of a shared table
/*
  This query retrieves the schema of the 'customer_info' table 
  that is part of the 'customer_data_share' Delta Share.
*/
DESCRIBE TABLE customer_info;

-- Query 4: Query data from a shared table
/*
  This query demonstrates how to query data from the 'customer_purchases' 
  table that is part of the 'customer_data_share' Delta Share.
*/
SELECT * FROM customer_purchases
WHERE purchase_amount > 100;

-- Query 5: Create a new table from a shared query
/*
  This query creates a new table named 'high_value_customers' 
  that contains the results of a query on the 'customer_purchases' 
  table from the 'customer_data_share' Delta Share.
*/
CREATE TABLE high_value_customers
AS
SELECT customer_id, SUM(purchase_amount) AS total_spent
FROM customer_purchases
WHERE purchase_amount > 100
GROUP BY customer_id;
```