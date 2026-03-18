# Screenshots

This folder contains BigQuery query results and 
visualizations from the Amazon Review Intelligence 
Pipeline analysis.

## Files

| File | Description |
|---|---|
| 01_dataset_overview.png | Query 1 — 481K rows, 100K products, 2001-2023 |
| 02_rating_distribution.png | J-curve — 59.8% of reviews are 5 stars |
| 03_divergence_signal.png | Products ranked by contradictory reviews |
| 04_review_velocity.png | Review volume 2001-2023 time series |
| 05_rating_trends_table.png | Avg rating declined 4.18 to 3.81 (2015-2021) |
| 06_bigquery_tables.png | BigQuery console showing both tables |

## Key Finding from Query 5

Rating quality in the Amazon Beauty category 
DECLINED between 2015 and 2021 — contradicting 
the common assumption of rating inflation:

| Year | Avg Rating | 5-Star % |
|------|------------|----------|
| 2015 | 4.18 | 63.0% |
| 2016 | 4.12 | 61.1% |
| 2017 | 4.03 | 59.7% |
| 2018 | 3.99 | 59.6% |
| 2019 | 4.05 | 62.9% |
| 2020 | 3.98 | 60.6% |
| 2021 | 3.81 | 56.4% |
| 2022 | 3.84 | 57.1% |
