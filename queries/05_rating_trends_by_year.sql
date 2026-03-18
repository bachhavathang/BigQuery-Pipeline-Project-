-- Query 5: Rating Quality Trends 2015-2023
-- Tests for rating inflation hypothesis
-- Finding: ratings DECLINED from 4.18 (2015) to 3.81 (2021)
SELECT
  EXTRACT(YEAR FROM timestamp) AS review_year,
  COUNT(*) AS total_reviews,
  COUNTIF(rating = 5) AS five_star,
  COUNTIF(rating = 1) AS one_star,
  ROUND(COUNTIF(rating = 5) / COUNT(*) * 100, 1)
    AS pct_five_star,
  ROUND(AVG(rating), 2) AS avg_rating
FROM `amazon-analysis-2015-2024.amazon_reviews.raw_reviews`
WHERE EXTRACT(YEAR FROM timestamp) >= 2015
GROUP BY review_year
ORDER BY review_year
