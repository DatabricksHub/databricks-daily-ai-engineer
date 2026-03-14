-- Query 1: Analyze customer interactions to identify common issues
/* 
This query analyzes customer interactions to identify common issues reported to the chatbot.
It groups the interactions by the intent identified by the chatbot and calculates the frequency of each intent.
The results can be used to improve the chatbot's training data and enhance its ability to resolve customer issues.
*/
SELECT 
  intent,
  COUNT(*) as frequency
FROM 
  customer_interactions
GROUP BY 
  intent
ORDER BY 
  frequency DESC;

-- Query 2: Evaluate chatbot performance based on customer satisfaction
/* 
This query evaluates the chatbot's performance based on customer satisfaction ratings.
It calculates the average satisfaction rating for each intent and identifies areas where the chatbot needs improvement.
The results can be used to refine the chatbot's responses and improve customer experience.
*/
SELECT 
  intent,
  AVG(satisfaction_rating) as average_satisfaction
FROM 
  customer_interactions
GROUP BY 
  intent
ORDER BY 
  average_satisfaction ASC;

-- Query 3: Identify top-performing chatbot responses
/* 
This query identifies the top-performing chatbot responses based on customer satisfaction ratings.
It selects the top 5 responses with the highest average satisfaction ratings and provides insights into what makes them effective.
The results can be used to optimize the chatbot's response generation and improve customer engagement.
*/
SELECT 
  response,
  AVG(satisfaction_rating) as average_satisfaction
FROM 
  customer_interactions
GROUP BY 
  response
ORDER BY 
  average_satisfaction DESC
LIMIT 5;

-- Query 4: Analyze conversation flow to identify bottlenecks
/* 
This query analyzes the conversation flow to identify bottlenecks and areas where customers are dropping off.
It calculates the conversation completion rate for each intent and identifies intents with low completion rates.
The results can be used to refine the chatbot's conversation flow and improve customer retention.
*/
SELECT 
  intent,
  SUM(CASE WHEN conversation_completed = 'true' THEN 1 ELSE 0 END) as completed_conversations,
  COUNT(*) as total_conversations,
  SUM(CASE WHEN conversation_completed = 'true' THEN 1 ELSE 0 END) / COUNT(*) as completion_rate
FROM 
  customer_interactions
GROUP BY 
  intent
ORDER BY 
  completion_rate ASC;

-- Query 5: Monitor chatbot usage and adoption
/* 
This query monitors chatbot usage and adoption over time.
It calculates the daily active users and conversation volume, providing insights into the chatbot's usage patterns and trends.
The results can be used to evaluate the chatbot's effectiveness and identify areas for improvement.
*/
SELECT 
  DATE(interaction_timestamp) as date,
  COUNT(DISTINCT user_id) as daily_active_users,
  COUNT(*) as conversation_volume
FROM 
  customer_interactions
GROUP BY 
  DATE(interaction_timestamp)
ORDER BY 
  date ASC;