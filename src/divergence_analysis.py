# Execution order:
# 1. pipeline.py
# 2. sentiment_scoring.py
# 3. divergence_analysis.py
# 4. gemini_extraction.py

"""
divergence_analysis.py — Calculates sentiment-rating divergence per product in SQL.

Runs a BigQuery query to identify products where star ratings and sentiment signals
diverge, assigns risk tiers, and saves results for downstream Gemini analysis.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


def load_environment():
    """Load environment variables from .env file."""
    load_dotenv()
    project = os.getenv("GCP_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    if not all([project, dataset]):
        raise EnvironmentError(
            "Missing required environment variables. "
            "Check .env for GCP_PROJECT, BQ_DATASET."
        )
    return project, dataset


def run_divergence_query(client: bigquery.Client) -> pd.DataFrame:
    """
    Execute the divergence analysis SQL query on BigQuery.

    Computes per-product sentiment-rating divergence metrics and assigns
    risk tiers (HIGH/MEDIUM/LOW) based on divergence percentage.

    Args:
        client: BigQuery client instance.

    Returns:
        DataFrame with divergence results sorted by divergence_magnitude DESC.
    """
    print("Running divergence analysis query...")

    query = """
    WITH product_stats AS (
      SELECT
        parent_asin,
        COUNT(*) AS review_count,
        ROUND(AVG(rating), 3) AS avg_star_rating,
        ROUND(AVG(sentiment_numeric), 3) AS avg_sentiment_numeric,
        ROUND(AVG(sentiment_score), 3) AS avg_sentiment_confidence,
        COUNTIF(rating >= 4 AND sentiment_numeric = -1) AS high_star_negative_sentiment,
        COUNTIF(rating <= 2 AND sentiment_numeric = 1) AS low_star_positive_sentiment,
        COUNT(*) AS total_reviews
      FROM `amazon-analysis-2015-2024.amazon_reviews.reviews_with_sentiment`
      GROUP BY parent_asin
      HAVING review_count >= 30
    ),
    divergence_scored AS (
      SELECT
        *,
        ROUND(
          (high_star_negative_sentiment + low_star_positive_sentiment)
          / total_reviews * 100, 2
        ) AS divergence_pct,
        ROUND(
          ABS(avg_star_rating/5 - (avg_sentiment_numeric + 1) / 2), 4
        ) AS divergence_magnitude
      FROM product_stats
    )
    SELECT
      parent_asin,
      review_count,
      avg_star_rating,
      avg_sentiment_numeric,
      avg_sentiment_confidence,
      high_star_negative_sentiment,
      low_star_positive_sentiment,
      divergence_pct,
      divergence_magnitude,
      CASE
        WHEN divergence_pct >= 20 THEN 'HIGH RISK'
        WHEN divergence_pct >= 10 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
      END AS risk_tier
    FROM divergence_scored
    ORDER BY divergence_magnitude DESC
    """

    df = client.query(query).to_dataframe()
    print(f"Query returned {df.shape[0]:,} products.")
    return df


def display_top_results(df: pd.DataFrame, n: int = 20):
    """
    Print the top N rows of the divergence results to console.

    Args:
        df: Divergence results DataFrame.
        n: Number of top rows to display.
    """
    print(f"\nTop {n} products by divergence_magnitude:")
    print("=" * 100)
    print(df.head(n).to_string(index=False))
    print("=" * 100)


def save_results(df: pd.DataFrame, output_path: str):
    """
    Save full divergence results to CSV.

    Args:
        df: Divergence results DataFrame.
        output_path: Path to save the CSV file.
    """
    df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}")


def print_summary(df: pd.DataFrame):
    """
    Print a summary of divergence analysis results.

    Shows total products, risk tier counts, and top 3 products
    by divergence_magnitude.

    Args:
        df: Divergence results DataFrame.
    """
    print("\n" + "=" * 60)
    print("DIVERGENCE ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Total products analyzed: {df.shape[0]:,}")

    risk_counts = df["risk_tier"].value_counts()
    print(f"HIGH RISK products:   {risk_counts.get('HIGH RISK', 0):,}")
    print(f"MEDIUM RISK products: {risk_counts.get('MEDIUM RISK', 0):,}")
    print(f"LOW RISK products:    {risk_counts.get('LOW RISK', 0):,}")

    print("\nTop 3 products by divergence_magnitude:")
    top3 = df.nlargest(3, "divergence_magnitude")[["parent_asin", "divergence_magnitude", "risk_tier"]]
    for _, row in top3.iterrows():
        print(f"  {row['parent_asin']}  |  magnitude: {row['divergence_magnitude']}  |  {row['risk_tier']}")
    print("=" * 60)


def main():
    """Main entry point for divergence analysis."""
    print("=" * 60)
    print("STEP 1: Loading environment variables...")
    project, dataset = load_environment()

    print("=" * 60)
    print("STEP 2: Connecting to BigQuery...")
    client = bigquery.Client(project=project)
    print("BigQuery client initialized.")

    print("=" * 60)
    print("STEP 3: Running divergence query...")
    df = run_divergence_query(client)

    print("=" * 60)
    print("STEP 4: Displaying top results...")
    display_top_results(df, n=20)

    print("=" * 60)
    print("STEP 5: Saving results...")
    output_path = os.path.join("outputs", "divergence_results.csv")
    save_results(df, output_path)

    print("STEP 6: Summary...")
    print_summary(df)

    print("\nDivergence analysis complete.")


if __name__ == "__main__":
    main()
