# Execution order:
# 1. pipeline.py
# 2. sentiment_scoring.py
# 3. divergence_analysis.py
# 4. gemini_extraction.py

"""
pipeline.py — Loads Amazon Beauty reviews from HuggingFace into BigQuery.

Downloads the McAuley-Lab Amazon-Reviews-2023 dataset (All_Beauty split),
cleans the data, caches locally, and writes to BigQuery for downstream analysis.
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
    table = os.getenv("BQ_TABLE")
    hf_token = os.getenv("HF_TOKEN")
    if not all([project, dataset, table, hf_token]):
        raise EnvironmentError(
            "Missing required environment variables. "
            "Check .env for GCP_PROJECT, BQ_DATASET, BQ_TABLE, HF_TOKEN."
        )
    return project, dataset, table, hf_token


def authenticate_huggingface(token: str):
    """Authenticate with HuggingFace Hub using the provided token."""
    from huggingface_hub import login
    login(token=token)
    print("HuggingFace authentication successful.")


def load_or_download_dataset(cache_path: str, max_rows: int = 500_000) -> pd.DataFrame:
    """
    Load dataset from local cache if available, otherwise download from HuggingFace.

    Args:
        cache_path: Path to the local CSV cache file.
        max_rows: Maximum number of rows to keep from the dataset.

    Returns:
        pandas DataFrame with raw review data.
    """
    if os.path.exists(cache_path):
        print("Cache found. Loading from local file.")
        df = pd.read_csv(cache_path)
        return df

    print("No cache found. Downloading dataset from HuggingFace...")
    from datasets import load_dataset
    dataset = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        "raw_review_All_Beauty",
        split="full",
        trust_remote_code=True,
    )
    df = dataset.to_pandas()
    df = df.head(max_rows)
    df.to_csv(cache_path, index=False)
    print(f"Downloaded and cached {max_rows:,} rows.")
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw reviews DataFrame.

    Drops rows with null rating/parent_asin/text, removes short texts,
    converts timestamps, and resets the index.

    Args:
        df: Raw reviews DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    print("Cleaning DataFrame...")
    initial_shape = df.shape[0]
    df = df.dropna(subset=["rating", "parent_asin", "text"])
    df = df[df["text"].str.len() >= 10]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.reset_index(drop=True)
    print(f"Cleaned: {initial_shape:,} -> {df.shape[0]:,} rows. Shape: {df.shape}")
    return df


def create_dataset_if_not_exists(client: bigquery.Client, project: str, dataset_id: str):
    """
    Create the BigQuery dataset if it does not already exist.

    Args:
        client: BigQuery client instance.
        project: GCP project ID.
        dataset_id: BigQuery dataset name.
    """
    dataset_ref = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset_ref.location = "US"
    try:
        client.create_dataset(dataset_ref)
        print(f"Created dataset: {project}.{dataset_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print("Dataset already exists, continuing.")
        else:
            raise


def write_to_bigquery(client: bigquery.Client, df: pd.DataFrame, table_ref: str):
    """
    Write a DataFrame to a BigQuery table with WRITE_TRUNCATE disposition.

    Args:
        client: BigQuery client instance.
        df: DataFrame to write.
        table_ref: Fully qualified BigQuery table reference.
    """
    print(f"Writing {df.shape[0]:,} rows to {table_ref}...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("BigQuery write completed.")
    except Exception as e:
        print(f"BigQuery write FAILED: {e}")
        raise


def verify_upload(client: bigquery.Client, table_ref: str):
    """
    Verify the BigQuery upload by querying the row count.

    Args:
        client: BigQuery client instance.
        table_ref: Fully qualified BigQuery table reference.
    """
    query = f"SELECT COUNT(*) AS row_count FROM `{table_ref}`"
    result = client.query(query).to_dataframe()
    n = result["row_count"].iloc[0]
    print(f"SUCCESS: {n:,} rows loaded into {table_ref}")


def main():
    """Main entry point for the data pipeline."""
    print("=" * 60)
    print("STEP 1: Loading environment variables...")
    project, dataset, table, hf_token = load_environment()

    print("=" * 60)
    print("STEP 2: Authenticating with HuggingFace...")
    authenticate_huggingface(hf_token)

    print("=" * 60)
    print("STEP 3: Loading dataset...")
    cache_path = os.path.join("outputs", "raw_reviews_cache.csv")
    df = load_or_download_dataset(cache_path)

    print("=" * 60)
    print("STEP 4: Cleaning data...")
    df = clean_dataframe(df)

    print("=" * 60)
    print("STEP 5: Setting up BigQuery...")
    client = bigquery.Client(project=project)
    create_dataset_if_not_exists(client, project, dataset)

    print("=" * 60)
    print("STEP 6: Writing to BigQuery...")
    table_ref = f"{project}.{dataset}.{table}"
    write_to_bigquery(client, df, table_ref)

    print("=" * 60)
    print("STEP 7: Verifying upload...")
    verify_upload(client, table_ref)

    print("=" * 60)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()
