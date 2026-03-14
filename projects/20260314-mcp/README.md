# README.md: Building a Recommender System using MCP and Databricks
==============================================

## Overview
This demo project showcases the capabilities of the Machine Learning Capability Platform (MCP) in Databricks by building a recommender system. The project demonstrates how to leverage MCP to discover, govern, and build machine learning models, and how to integrate it with Databricks for scalable and efficient model development.

## Why this feature matters
The MCP feature matters because it enables data engineers and data scientists to streamline their machine learning workflows, making it easier to build, deploy, and manage models. By using MCP in Databricks, users can take advantage of a unified platform for data engineering, data science, and machine learning, leading to faster time-to-insight and more accurate model predictions.

## Prerequisites
Before running this demo project, ensure you have:
* A Databricks account with MCP enabled
* A cluster with the necessary dependencies installed (e.g., `databricks-ml`)
* A sample dataset for building the recommender system (e.g., user-item interactions)

## How to run
To run this demo project, follow these steps:

1. **Notebook**: Open the `Recommender System` notebook in your Databricks workspace and run each cell in sequence. The notebook will guide you through the process of:
	* Importing necessary libraries and loading the sample dataset
	* Preprocessing the data and splitting it into training and testing sets
	* Building and training a recommender system model using MCP
	* Evaluating the model's performance and generating recommendations
2. **SQL**: Additionally, you can use SQL to query the model's predictions and recommendations. Run the following SQL query in a new Databricks SQL cell:
```sql
SELECT * FROM recommendations;
```
This will display the top-N recommended items for each user.

## Expected output
After running the notebook and SQL query, you should see the following output:
* A trained recommender system model with evaluated performance metrics (e.g., precision, recall, F1-score)
* A list of top-N recommended items for each user, along with their corresponding prediction scores

## Links to Databricks documentation
For more information on MCP and Databricks, refer to the following resources:
* [Databricks MCP documentation](https://docs.databricks.com/machine-learning/mcp/index.html)
* [Databricks Machine Learning documentation](https://docs.databricks.com/machine-learning/index.html)
* [Databricks SQL documentation](https://docs.databricks.com/sql/index.html)