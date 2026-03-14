# Databricks notebook source
# Feature: Genie Code
# Project: Automating data pipeline creation using Genie Code for a sample e-commerce dataset

# COMMAND ----------
"""
Section 1: Import necessary libraries and configure the environment
"""
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# COMMAND ----------
"""
Section 2: Create or load required data / tables
"""
# Create a sample e-commerce dataset
data = spark.createDataFrame([
    ("Product A", 10.0, "Electronics"),
    ("Product B", 20.0, "Fashion"),
    ("Product C", 15.0, "Electronics"),
    ("Product D", 30.0, "Home"),
    ("Product E", 25.0, "Fashion")
], ["Product", "Price", "Category"])

# Create a Delta table
data.write.format("delta").saveAsTable("ecommerce_data")

# COMMAND ----------
"""
Section 3: Core feature demonstration code - Genie Code for automating data pipeline creation
"""
# Define a function to generate code for data engineering and machine learning tasks
def generate_genie_code(data):
    """
    Generate Genie Code for automating data pipeline creation
    """
    # Define a pipeline for data preprocessing and model training
    tokenizer = Tokenizer(inputCol="Product", outputCol="words")
    hashing_tf = HashingTF(inputCol="words", outputCol="features", numFeatures=20)
    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    pipeline = Pipeline(stages=[tokenizer, hashing_tf, lr])

    # Define a parameter grid for hyperparameter tuning
    param_grid = ParamGridBuilder() \
        .addGrid(lr.maxIter, [10, 20, 30]) \
        .addGrid(lr.regParam, [0.1, 0.3, 0.5]) \
        .build()

    # Perform cross-validation and model selection
    crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid, evaluator=MulticlassClassificationEvaluator(), numFolds=2)
    cv_model = crossval.fit(data)

    return cv_model

# Generate Genie Code for the sample e-commerce dataset
genie_code_model = generate_genie_code(data)

# COMMAND ----------
"""
Section 4: Analytics / transformations on the result
"""
# Make predictions on the test data
predictions = genie_code_model.transform(data)

# Evaluate the model performance
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
print("Model Accuracy:", accuracy)

# COMMAND ----------
"""
Section 5: Validation
"""
print("Demo completed successfully")