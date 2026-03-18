# Execution order:
# 1. pipeline.py
# 2. sentiment_scoring.py
# 3. divergence_analysis.py
# 4. gemini_extraction.py

"""
gemini_extraction.py — Uses Gemini Flash to extract complaint themes from HIGH RISK products.

Reads divergence results, fetches the most negative reviews for high-risk products,
and uses Gemini 1.5 Flash to identify recurring complaint themes.
"""

import os
import json
import time
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google import genai
from google.genai import types


def load_environment():
    """Load environment variables from .env file and validate Gemini API key."""
    load_dotenv()
    project = os.getenv("GCP_PROJECT")
    dataset = os.getenv("BQ_DATASET")
    gemini_key = os.getenv("GEMINI_API_KEY")

    if not gemini_key or gemini_key == "your_gemini_api_key_here":
        print("ERROR: GEMINI_API_KEY is missing or not set in .env file.")
        print("Please add your Gemini API key to the .env file and try again.")
        exit(1)

    if not all([project, dataset]):
        raise EnvironmentError(
            "Missing required environment variables. "
            "Check .env for GCP_PROJECT, BQ_DATASET."
        )

    return project, dataset, gemini_key


def load_high_risk_products(csv_path: str, max_products: int = 20) -> pd.DataFrame:
    """
    Load divergence results and filter to HIGH RISK products.

    Args:
        csv_path: Path to divergence_results.csv.
        max_products: Maximum number of high-risk products to process.

    Returns:
        DataFrame of top high-risk products sorted by divergence_magnitude.
    """
    print(f"Loading divergence results from {csv_path}...")
    df = pd.read_csv(csv_path)
    high_risk = df[df["risk_tier"].isin(["HIGH RISK", "MEDIUM RISK"])].sort_values(
        "divergence_magnitude", ascending=False
    )

    if len(high_risk) < max_products:
        print(f"Found {len(high_risk)} HIGH RISK + MEDIUM RISK products (fewer than {max_products}).")
    else:
        high_risk = high_risk.head(max_products)
        print(f"Selected top {max_products} HIGH RISK products.")

    return high_risk.reset_index(drop=True)


def fetch_negative_reviews(
    client: bigquery.Client,
    project: str,
    dataset: str,
    parent_asin: str,
    max_reviews: int = 30,
) -> list:
    """
    Fetch the most negative reviews for a product from BigQuery.

    Queries reviews where sentiment_numeric = -1,
    ordered by sentiment_score descending.

    Args:
        client: BigQuery client instance.
        project: GCP project ID.
        dataset: BigQuery dataset name.
        parent_asin: Product ASIN to query.
        max_reviews: Maximum number of reviews to fetch.

    Returns:
        List of review text strings.
    """
    query = f"""
        SELECT text
        FROM `{project}.{dataset}.reviews_with_sentiment`
        WHERE parent_asin = @parent_asin
          AND sentiment_numeric = -1
        ORDER BY sentiment_score DESC
        LIMIT {max_reviews}
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("parent_asin", "STRING", parent_asin)
        ]
    )
    result = client.query(query, job_config=job_config).to_dataframe()
    return result["text"].tolist()


def build_review_block(reviews: list, max_chars: int = 8000) -> str:
    """
    Concatenate reviews into a single string, truncated to max_chars.

    Args:
        reviews: List of review text strings.
        max_chars: Maximum character length for the combined block.

    Returns:
        Concatenated and truncated review string.
    """
    combined = "\n---\n".join(reviews)
    if len(combined) > max_chars:
        combined = combined[:max_chars]
    return combined


def call_gemini(model, review_block: str) -> dict:
    """
    Call Gemini Flash API to extract complaint themes from reviews.

    Args:
        model: Gemini GenerativeModel instance.
        review_block: Concatenated review text.

    Returns:
        Parsed JSON dict with themes, or dict with 'error' key on failure.
    """
    prompt = (
        "You are a product analyst. Below are customer reviews "
        "for a product that received high star ratings but "
        "negative sentiment signals. Analyze these reviews and "
        "extract exactly 3 specific complaint themes that "
        "explain the disconnect.\n\n"
        "For each theme provide:\n"
        "- theme_name: 2-4 word label\n"
        "- description: one sentence explanation\n"
        "- frequency_signal: 'common' or 'occasional'\n\n"
        "Respond in valid JSON only. No explanation outside "
        "the JSON. No markdown code blocks. Raw JSON only.\n\n"
        "Format:\n"
        "{\n"
        "  'themes': [\n"
        "    {\n"
        "      'theme_name': '...',\n"
        "      'description': '...',\n"
        "      'frequency_signal': '...'\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        f"Reviews:\n{review_block}"
    )

    response = model.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )
    raw_text = response.text.strip()

    try:
        parsed = json.loads(raw_text)
        return parsed
    except json.JSONDecodeError:
        cleaned = raw_text.replace("'", '"')
        try:
            parsed = json.loads(cleaned)
            return parsed
        except json.JSONDecodeError:
            return {"error": "PARSE_ERROR", "raw": raw_text}


def extract_themes_for_products(
    high_risk_df: pd.DataFrame,
    client: bigquery.Client,
    model,
    project: str,
    dataset: str,
) -> pd.DataFrame:
    """
    Process each HIGH RISK product: fetch reviews, call Gemini, extract themes.

    Args:
        high_risk_df: DataFrame of high-risk products.
        client: BigQuery client instance.
        model: Gemini client object.
        project: GCP project ID.
        dataset: BigQuery dataset name.

    Returns:
        DataFrame with columns: parent_asin, risk_tier, divergence_pct,
        divergence_magnitude, theme_1, theme_2, theme_3.
    """
    results = []

    for idx, row in high_risk_df.iterrows():
        asin = row["parent_asin"]
        print(f"Processing product {idx + 1}/{len(high_risk_df)}: {asin}...")

        reviews = fetch_negative_reviews(client, project, dataset, asin)
        if not reviews:
            print(f"  No negative reviews found for {asin}. Skipping.")
            results.append({
                "parent_asin": asin,
                "risk_tier": row["risk_tier"],
                "divergence_pct": row["divergence_pct"],
                "divergence_magnitude": row["divergence_magnitude"],
                "theme_1": "NO_REVIEWS",
                "theme_2": "NO_REVIEWS",
                "theme_3": "NO_REVIEWS",
            })
            continue

        review_block = build_review_block(reviews)
        gemini_result = call_gemini(model, review_block)

        if "error" in gemini_result:
            print(f"  PARSE_ERROR for product {asin}. Storing error and continuing.")
            results.append({
                "parent_asin": asin,
                "risk_tier": row["risk_tier"],
                "divergence_pct": row["divergence_pct"],
                "divergence_magnitude": row["divergence_magnitude"],
                "theme_1": "PARSE_ERROR",
                "theme_2": "PARSE_ERROR",
                "theme_3": "PARSE_ERROR",
            })
        else:
            themes = gemini_result.get("themes", [])
            theme_strs = [str(t) for t in themes]
            while len(theme_strs) < 3:
                theme_strs.append("N/A")
            results.append({
                "parent_asin": asin,
                "risk_tier": row["risk_tier"],
                "divergence_pct": row["divergence_pct"],
                "divergence_magnitude": row["divergence_magnitude"],
                "theme_1": theme_strs[0],
                "theme_2": theme_strs[1],
                "theme_3": theme_strs[2],
            })
            print(f"  Extracted {len(themes)} themes for {asin}.")

        time.sleep(2)

    return pd.DataFrame(results)


def main():
    """Main entry point for Gemini complaint theme extraction."""
    print("=" * 60)
    print("STEP 1: Loading environment variables...")
    project, dataset, gemini_key = load_environment()

    print("=" * 60)
    print("STEP 2: Loading HIGH RISK products...")
    csv_path = os.path.join("outputs", "divergence_results.csv")
    high_risk_df = load_high_risk_products(csv_path)

    print("=" * 60)
    print("STEP 3: Initializing BigQuery and Gemini...")
    client = bigquery.Client(project=project)
    client_gemini = genai.Client(api_key=gemini_key)
    model = client_gemini
    print("BigQuery client and Gemini model initialized.")

    print("=" * 60)
    print("STEP 4: Extracting complaint themes...")
    results_df = extract_themes_for_products(
        high_risk_df, client, model, project, dataset
    )

    print("=" * 60)
    print("STEP 5: Saving results...")
    output_path = os.path.join("outputs", "gemini_complaint_themes.csv")
    results_df.to_csv(output_path, index=False)

    print("=" * 60)
    print(f"Gemini extraction complete. Results saved to {output_path}")


if __name__ == "__main__":
    main()
