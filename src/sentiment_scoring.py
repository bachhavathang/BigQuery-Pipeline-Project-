# Execution order:
# 1. pipeline.py
# 2. sentiment_scoring.py
# 3. divergence_analysis.py
# 4. gemini_extraction.py

"""
sentiment_scoring.py — Adds DistilBERT sentiment scores to BigQuery table.

Pulls reviews from BigQuery in batches, runs DistilBERT sentiment analysis,
and writes enriched results to a new BigQuery table.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from transformers import pipeline as hf_pipeline


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


def initialize_sentiment_pipeline():
    """
    Load the DistilBERT sentiment analysis pipeline once.

    Returns:
        HuggingFace sentiment-analysis pipeline.
    """
    print("Loading DistilBERT sentiment model...")
    sentiment_pipe = hf_pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        truncation=True,
        max_length=512,
    )
    print("Sentiment model loaded.")
    return sentiment_pipe


def fetch_reviews_in_batches(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    batch_size: int = 256,
    max_rows: int = 100_000,
) -> pd.DataFrame:
    """
    Pull reviews from BigQuery in batches using LIMIT/OFFSET pagination.

    Args:
        client: BigQuery client instance.
        project: GCP project ID.
        dataset: BigQuery dataset name.
        table: BigQuery table name.
        batch_size: Number of rows per batch.
        max_rows: Maximum total rows to fetch.

    Returns:
        DataFrame with all fetched reviews.
    """
    table_ref = f"`{project}.{dataset}.{table}`"
    all_batches = []
    total_batches = (max_rows + batch_size - 1) // batch_size
    offset = 0
    batch_num = 0

    print(f"Fetching up to {max_rows:,} reviews in batches of {batch_size}...")
    while offset < max_rows:
        query = f"""
            SELECT rating, text, parent_asin
            FROM {table_ref}
            WHERE text IS NOT NULL AND LENGTH(text) > 10
            LIMIT {batch_size} OFFSET {offset}
        """
        batch_df = client.query(query).to_dataframe()
        if batch_df.empty:
            print(f"No more rows at offset {offset}. Stopping.")
            break

        all_batches.append(batch_df)
        batch_num += 1

        if batch_num % 10 == 0:
            print(f"Processing batch {batch_num} of {total_batches}...")

        offset += batch_size

    combined = pd.concat(all_batches, ignore_index=True)
    print(f"Fetched {combined.shape[0]:,} total reviews in {batch_num} batches.")
    return combined


def score_sentiment(df: pd.DataFrame, sentiment_pipe) -> pd.DataFrame:
    """
    Run sentiment scoring on all review texts.

    Adds sentiment_label, sentiment_score, and sentiment_numeric columns.

    Args:
        df: DataFrame with a 'text' column.
        sentiment_pipe: HuggingFace sentiment-analysis pipeline.

    Returns:
        DataFrame with sentiment columns added.
    """
    print(f"Running sentiment scoring on {df.shape[0]:,} reviews...")
    texts = df["text"].tolist()

    batch_size = 256
    all_results = []
    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i : i + batch_size]
        results = sentiment_pipe(batch_texts)
        all_results.extend(results)
        if (i // batch_size + 1) % 10 == 0:
            print(f"  Scored {i + len(batch_texts):,} / {len(texts):,} reviews...")

    df["sentiment_label"] = [r["label"] for r in all_results]
    df["sentiment_score"] = [round(r["score"], 4) for r in all_results]
    df["sentiment_numeric"] = [1 if r["label"] == "POSITIVE" else -1 for r in all_results]

    print("Sentiment scoring complete.")
    return df


def write_sentiment_to_bigquery(
    client: bigquery.Client, df: pd.DataFrame, project: str, dataset: str
):
    """
    Write sentiment-enriched DataFrame to a new BigQuery table.

    Args:
        client: BigQuery client instance.
        df: DataFrame with sentiment columns.
        project: GCP project ID.
        dataset: BigQuery dataset name.
    """
    table_ref = f"{project}.{dataset}.reviews_with_sentiment"
    print(f"Writing {df.shape[0]:,} rows to {table_ref}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"SUCCESS: {df.shape[0]:,} rows written to {table_ref}")
    except Exception as e:
        print(f"BigQuery write FAILED: {e}")
        raise


def main():
    """Main entry point for sentiment scoring pipeline."""
    print("=" * 60)
    print("STEP 1: Loading environment variables...")
    project, dataset = load_environment()

    print("=" * 60)
    print("STEP 2: Connecting to BigQuery...")
    client = bigquery.Client(project=project)
    print("BigQuery client initialized.")

    print("=" * 60)
    print("STEP 3: Fetching reviews from BigQuery...")
    table = os.getenv("BQ_TABLE", "raw_reviews")
    df = fetch_reviews_in_batches(client, project, dataset, table)

    print("=" * 60)
    print("STEP 4: Loading sentiment model...")
    sentiment_pipe = initialize_sentiment_pipeline()

    print("=" * 60)
    print("STEP 5: Scoring sentiment...")
    df = score_sentiment(df, sentiment_pipe)

    print("=" * 60)
    print("STEP 6: Preparing final DataFrame...")
    df = df[["parent_asin", "rating", "text", "sentiment_label", "sentiment_score", "sentiment_numeric"]]
    print(f"Final DataFrame shape: {df.shape}")

    print("=" * 60)
    print("STEP 7: Writing to BigQuery...")
    write_sentiment_to_bigquery(client, df, project, dataset)

    print("=" * 60)
    print("STEP 8: Verifying row count...")
    table_ref = f"{project}.{dataset}.reviews_with_sentiment"
    query = f"SELECT COUNT(*) AS row_count FROM `{table_ref}`"
    result = client.query(query).to_dataframe()
    n = result["row_count"].iloc[0]
    print(f"Confirmed: {n:,} rows in {table_ref}")

    print("=" * 60)
    print("Sentiment scoring pipeline complete.")


if __name__ == "__main__":
    main()
