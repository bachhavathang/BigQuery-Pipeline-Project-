-- Query 2: Sentiment Distribution
-- Shows POSITIVE/NEGATIVE split and how sentiment aligns with star ratings
SELECT
  sentiment_label,
  COUNT(*) AS review_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1) AS pct,
  ROUND(AVG(rating), 2) AS avg_star_rating
FROM `amazon-analysis-2015-2024.amazon_reviews.reviews_with_sentiment`
GROUP BY sentiment_label
ORDER BY review_count DESC
