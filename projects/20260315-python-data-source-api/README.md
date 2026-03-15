# README.md: Custom Data Connector using Python Data Source API
## Overview
This demo project showcases the use of the Python Data Source API in Databricks to create a custom data connector for a proprietary data format. The project demonstrates how to leverage this feature to integrate diverse data formats and systems, enabling seamless data ingestion and processing within the Databricks ecosystem.

## Why this feature matters
The Python Data Source API is a powerful tool that allows data engineers to extend the capabilities of Databricks, supporting the integration of custom and proprietary data formats. This feature is crucial for organizations that work with unique data sources, as it enables them to:

* Unlock insights from diverse data formats
* Simplify data ingestion and processing pipelines
* Enhance data governance and security

## Prerequisites
To run this demo project, you will need:

* A Databricks workspace with a cluster configured
* Python 3.7 or later installed
* The `databricks` library installed (`pip install databricks`)
* A sample dataset in the proprietary data format (provided with the project)

## How to run
To run this demo project, follow these steps:

1. **Notebook**: Open the `custom_data_connector` notebook in your Databricks workspace and run all cells. This will create a custom data source using the Python Data Source API.
2. **SQL**: Run the following SQL query to verify the custom data source:
```sql
SELECT * FROM custom_data_source;
```
This query will display the data ingested from the proprietary data format.

## Expected output
The expected output will be a table displaying the data from the proprietary data format, demonstrating successful integration with Databricks using the custom data connector.

## Additional resources
For more information on the Python Data Source API and Databricks, please refer to the following resources:

* [Databricks Python Data Source API documentation](https://docs.databricks.com/data/data-sources/python.html)
* [Databricks Data Engineering documentation](https://docs.databricks.com/data-engineering/index.html)
* [Databricks custom data sources documentation](https://docs.databricks.com/data/data-sources/custom.html)