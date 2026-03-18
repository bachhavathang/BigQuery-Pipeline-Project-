-- Query 3: Core Divergence Signal by Product
-- Ranks products by contradictory reviews (high star + negative sentiment)
SELECT
  parent_asin,
  COUNT(*) AS review_count,
  ROUND(AVG(rating), 2) AS avg_star_rating,
  ROUND(AVG(sentiment_numeric), 2) AS avg_sentiment,
  COUNTIF(rating >= 4 AND sentiment_numeric = -1)
    AS high_star_negative,
  COUNTIF(rating <= 2 AND sentiment_numeric = 1)
    AS low_star_positive
FROM `amazon-analysis-2015-2024.amazon_reviews.reviews_with_sentiment`
GROUP BY parent_asin
HAVING review_count >= 30
ORDER BY high_star_negative DESC
LIMIT 20
