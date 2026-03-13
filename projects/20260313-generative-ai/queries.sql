-- Query 1: Exploring the distribution of customer feedback categories
/* 
This query calculates the count of each category in the customer feedback data.
It helps to understand the class balance and distribution of the data, 
which is essential for building an effective text classification model.
*/
SELECT category, COUNT(*) as count 
FROM customer_feedback 
GROUP BY category;

-- Query 2: Preprocessing and tokenizing the text data
/* 
This query preprocesses the text data by converting it to lowercase, 
removing special characters and punctuation, and tokenizing the text into words.
It prepares the data for training the text classification model.
*/
SELECT 
  id,
  LOWER(text) as text,
  REGEXP_REPLACE(text, '[^a-zA-Z0-9\\s]', '') as cleaned_text,
  SPLIT(cleaned_text, ' ') as tokens
FROM customer_feedback;

-- Query 3: Evaluating the performance of the text classification model
/* 
This query calculates the accuracy, precision, and recall of the text classification model.
It helps to evaluate the performance of the model and identify areas for improvement.
*/
SELECT 
  SUM(IF(predicted_category = actual_category, 1, 0)) / COUNT(*) as accuracy,
  SUM(IF(predicted_category = actual_category AND predicted_category = 'positive', 1, 0)) / 
  SUM(IF(predicted_category = 'positive', 1, 0)) as precision,
  SUM(IF(predicted_category = actual_category AND actual_category = 'positive', 1, 0)) / 
  SUM(IF(actual_category = 'positive', 1, 0)) as recall
FROM model_predictions;

-- Query 4: Analyzing the most common words in positive and negative feedback
/* 
This query calculates the most common words in positive and negative customer feedback.
It helps to identify the key phrases and words that are associated with each category.
*/
SELECT 
  category,
  word,
  COUNT(*) as frequency
FROM (
  SELECT 
    category,
    SPLIT(cleaned_text, ' ') as words
  FROM customer_feedback
) 
EXPLODE(words) as word
GROUP BY category, word
ORDER BY frequency DESC;

-- Query 5: Identifying the most informative features for the model
/* 
This query calculates the mutual information between each feature and the target variable.
It helps to identify the most informative features that contribute to the model's performance.
*/
SELECT 
  feature,
  MUTUAL_INFORMATION(feature, category) as mutual_info
FROM (
  SELECT 
    category,
    TOKENS(cleaned_text) as features
  FROM customer_feedback
) 
LATERAL VIEW EXPLODE(features) as feature
GROUP BY feature
ORDER BY mutual_info DESC;