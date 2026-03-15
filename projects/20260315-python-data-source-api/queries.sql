/* This query demonstrates reading data from a custom data source using the Python Data Source API */
SELECT * FROM custom_data_source_table 
-- This table is assumed to be created in the companion notebook using the Python Data Source API

/* This query showcases filtering data from the custom data source based on a specific condition */
SELECT * FROM custom_data_source_table 
WHERE column1 > 100 
-- This query filters data from the custom data source table where the value in column1 is greater than 100

/* This query highlights aggregating data from the custom data source to gain insights */
SELECT column2, SUM(column3) AS total 
FROM custom_data_source_table 
GROUP BY column2 
-- This query aggregates data from the custom data source table by grouping it based on column2 and calculating the sum of column3

/* This query demonstrates joining the custom data source with another table to combine data */
SELECT * 
FROM custom_data_source_table 
JOIN another_table 
ON custom_data_source_table.column1 = another_table.column4 
-- This query joins the custom data source table with another table based on a common column to combine data from both tables

/* This query shows creating a temporary view from the custom data source for further analysis */
CREATE TEMPORARY VIEW custom_data_view 
AS 
SELECT * 
FROM custom_data_source_table 
WHERE column1 > 50 
-- This query creates a temporary view from the custom data source table with filtered data for further analysis