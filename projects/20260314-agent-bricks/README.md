# Agent Bricks Demo Project
## Overview
This demo project showcases the Agent Bricks feature in Databricks, which accelerates AI development by providing a platform to discover, govern, and build AI models with Model Serving and Management Platform (MCP). The project demonstrates how to build a chatbot using Agent Bricks and MCP to provide AI-driven customer support.

## Why this feature matters
Agent Bricks is a game-changer for AI development, as it enables data scientists and engineers to:
* Discover and manage AI models in a centralized platform
* Govern AI models with version control and auditing
* Build and deploy AI models with ease, using a simple and intuitive interface
With Agent Bricks, organizations can accelerate their AI development and deployment, leading to faster time-to-market and improved customer experiences.

## Prerequisites
To run this demo project, you will need:
* A Databricks account with access to the Agent Bricks feature
* A basic understanding of Databricks notebooks and SQL
* The MCP platform set up and configured

## How to run
To run this demo project, follow these steps:
1. **Notebook**: Open the `AgentBricksChatbot` notebook in your Databricks workspace and run each cell in sequence. This will create and train the AI model using Agent Bricks.
2. **SQL**: Run the `create_chatbot_table` SQL query to create a table for storing chatbot interactions.
3. **Model Deployment**: Deploy the trained model to the MCP platform using the `deploy_model` notebook cell.

## Expected output
After running the demo project, you should see:
* A trained AI model that can respond to basic customer support queries
* A chatbot interface that allows users to interact with the AI model
* A table in your Databricks database that stores chatbot interactions

## Additional Resources
For more information on Agent Bricks and MCP, please refer to the following Databricks documentation:
* [Agent Bricks documentation](https://docs.databricks.com/machine-learning/agent-bricks/index.html)
* [MCP documentation](https://docs.databricks.com/machine-learning/model-serving/index.html)
* [Databricks notebooks documentation](https://docs.databricks.com/notebooks/index.html)
* [Databricks SQL documentation](https://docs.databricks.com/sql/index.html)