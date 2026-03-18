-- Query 4: Rating Distribution (J-Curve)
-- Shows the classic J-curve pattern in consumer marketplace reviews
SELECT
  CAST(rating AS INT64) AS star_rating,
  COUNT(*) AS review_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)
    AS pct_of_total
FROM `amazon-analysis-2015-2024.amazon_reviews.raw_reviews`
GROUP BY star_rating
ORDER BY star_rating
