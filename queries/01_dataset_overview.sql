-- Query 1: Dataset Overview
-- Shows total scale, unique products, avg rating, and date range
SELECT
  COUNT(*) AS total_reviews,
  COUNT(DISTINCT parent_asin) AS unique_products,
  ROUND(AVG(rating), 2) AS avg_rating,
  MIN(timestamp) AS earliest_review,
  MAX(timestamp) AS latest_review
FROM `amazon-analysis-2015-2024.amazon_reviews.raw_reviews`
