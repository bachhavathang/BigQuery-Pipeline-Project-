# Amazon Review Intelligence Pipeline

**End-to-end product analytics pipeline that ingests 481K Amazon Beauty reviews into Google BigQuery, enriches with DistilBERT sentiment scoring, detects listing quality risk through sentiment-rating divergence analysis, and extracts structured complaint themes using Gemini 2.5 Flash.**

---

## The Business Problem

Star ratings are a blunt instrument. A product averaging 4.2 stars can be hiding a real quality problem — customers who feel obligated to rate generously but write negatively. The platform sees a healthy score; the actual words tell a different story.

This project asks: *which products show the largest gap between what customers rate and what they actually say — and what specific complaints explain that disconnect?*

This matters for platform teams because undetected listing quality risk inflates conversion rates, increases returns, and erodes customer trust over time. For sellers, it surfaces fixable problems before they compound into review spirals.

---

## What This Project Does

- Ingests 481,156 Amazon Beauty reviews from HuggingFace into Google BigQuery via a modular Python pipeline
- Scores 100,096 reviews with DistilBERT sentiment model (POSITIVE/NEGATIVE + confidence score per review)
- Runs a 3-CTE BigQuery SQL query to calculate sentiment-rating divergence per product
- Assigns risk tiers (HIGH / MEDIUM / LOW RISK) based on the percentage of reviews where rating and sentiment contradict each other
- Extracts 3 structured complaint themes per flagged product using Gemini 2.5 Flash

---

## Results

| Risk Tier | Products | Threshold | Signal |
|:---|:---:|:---|:---|
| HIGH RISK | 3 | divergence_pct >= 20% | 20%+ of reviews contradict their own star rating |
| MEDIUM RISK | 49 | divergence_pct >= 10% | Meaningful sentiment-rating gap, warrants monitoring |
| LOW RISK | 178 | divergence_pct < 10% | Rating and sentiment broadly aligned |

**Total products analyzed:** 230 (minimum 30 reviews per product required)

**Key finding:** The Beauty category shows relatively low divergence overall — most poorly-rated products are also written about negatively, meaning the star signal is largely honest. The 52 flagged products (3 HIGH + 49 MEDIUM RISK) represent cases where the rating is actively misleading relative to review language — the highest-priority targets for listing quality investigation.

**Gemini extraction output:** Top 20 flagged products processed, 60 structured complaint themes extracted. Sample themes across products:

| Product | Theme | Description |
|:---|:---|:---|
| B000PKKAGO | Poor Shave Quality | Product consistently fails to deliver a close, efficient shave |
| B000PKKAGO | Design & Usability Flaws | Practical design issues create friction in everyday use |
| B000G8LWZI | Skin Irritation & Breakouts | Users report adverse reactions including breakouts after use |
| B004PZXJUO | Short Cooling Duration | Product fails to maintain its cooling effect for a satisfactory period |
| B004PZXJUO | Smaller Than Expected | Physical size significantly smaller than listing suggests |

---
## Query Results & Visuals

### Dataset Overview
481,156 reviews across 100,170 unique products spanning 
May 1996 to September 2023. Average rating: 3.98 stars.

![Dataset Overview](screenshots/01_dataset_overview.png)

### Rating Distribution — The J-Curve
59.8% of all Beauty reviews are 5 stars. 
Only 13.4% are 1 star. This J-curve is typical of 
consumer marketplaces — but it masks the divergence 
signal this project is designed to detect.

![Rating Distribution](screenshots/02_rating_distribution.png)

### Rating Quality Decline 2015–2023
Contrary to the assumption of rating inflation, 
average ratings in Beauty DECLINED from 4.18 in 2015 
to 3.81 in 2021 — a 0.37 point drop over 7 years. 
Five-star percentage fell from 63% to 56.4% over 
the same period, with 2021 seeing the highest 
one-star count (14,770 reviews).

| Year | Reviews | 5-Star % | Avg Rating |
|------|---------|----------|------------|
| 2015 | 24,531 | 63.0% | 4.18 |
| 2016 | 43,575 | 61.1% | 4.12 |
| 2017 | 47,394 | 59.7% | 4.03 |
| 2018 | 51,452 | 59.6% | 3.99 |
| 2019 | 69,136 | 62.9% | 4.05 |
| 2020 | 86,471 | 60.6% | 3.98 |
| 2021 | 84,338 | 56.4% | 3.81 |
| 2022 | 42,834 | 57.1% | 3.84 |

### Divergence Signal by Product
Products ranked by contradictory reviews — 
high star rating paired with negative sentiment language.

![Divergence Signal](screenshots/03_divergence_signal.png)

### Review Velocity 2001–2023
Near-zero activity until 2012, explosive growth 
peaking at ~10,000 reviews/month in 2019-2020, 
sharp decline post-2021 reflecting both dataset 
cutoff and post-COVID market normalization.

![Review Velocity](screenshots/04_review_velocity.png)

---

## Pipeline

```
pipeline.py             -> 481,156 reviews via HuggingFace -> BigQuery raw_reviews table
sentiment_scoring.py    -> 100,096 reviews scored with DistilBERT -> reviews_with_sentiment table
divergence_analysis.py  -> BigQuery SQL (3-CTE query) -> 230 products risk-scored -> divergence_results.csv
gemini_extraction.py    -> Gemini 2.5 Flash -> top 20 products x 3 themes -> gemini_complaint_themes.csv
```

---

## Divergence Metric

The analytical signal is built from two complementary measures:

**divergence_pct** — percentage of a product's reviews that are contradictory:
```
(high_star_negative_sentiment + low_star_positive_sentiment) / total_reviews x 100
```

**divergence_magnitude** — absolute gap between normalized star rating and normalized sentiment:
```
ABS(avg_star_rating / 5 - (avg_sentiment_numeric + 1) / 2)
```

Products are risk-tiered by `divergence_pct` and ranked by `divergence_magnitude`. The dual-signal approach separates products with a few extreme outlier reviews from those with a systematic, consistent contradiction between rating language and star score.

---

## Tech Stack

| Layer | Technology |
|:---|:---|
| Cloud Data Warehouse | Google BigQuery |
| Data Ingestion | HuggingFace Datasets SDK, Python |
| Sentiment Model | DistilBERT (distilbert-base-uncased-finetuned-sst-2-english) |
| Divergence Analysis | BigQuery SQL — 3-CTE analytical query |
| LLM Theme Extraction | Gemini 2.5 Flash via google-genai SDK |
| Authentication | GCP Application Default Credentials (ADC) |
| Language | Python 3.11 |

---

## Setup

```bash
git clone https://github.com/bachhavathang/BigQuery-Pipeline-Project-.git
cd BigQuery-Pipeline-Project-
python -m venv .venv
source .venv/bin/activate   # Mac/Linux
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
gcloud auth application-default login
```

Create a `.env` file in the project root:

```
GCP_PROJECT=your-gcp-project-id
BQ_DATASET=amazon_reviews
BQ_TABLE=raw_reviews
SENTIMENT_TABLE=reviews_with_sentiment
HF_TOKEN=your_huggingface_token
GEMINI_API_KEY=your_gemini_api_key
```

Get your HuggingFace token at huggingface.co/settings/tokens.
Get your Gemini API key at aistudio.google.com.

Run the full pipeline in order:

```bash
python src/pipeline.py
python src/sentiment_scoring.py
python src/divergence_analysis.py
python src/gemini_extraction.py
```

---

## Project Structure

```
BigQuery-Pipeline-Project-/
├── src/
│   ├── pipeline.py               <- HuggingFace -> BigQuery ingestion
│   ├── sentiment_scoring.py      <- DistilBERT sentiment enrichment
│   ├── divergence_analysis.py    <- BigQuery SQL risk scoring
│   └── gemini_extraction.py      <- Gemini complaint theme extraction
├── notebooks/
│   └── exploration.ipynb         <- EDA, BigQuery verification, visualizations
├── outputs/
│   ├── raw_reviews_cache.csv     <- generated at runtime (500K rows cached locally)
│   ├── divergence_results.csv    <- generated at runtime (230 products, risk-scored)
│   └── gemini_complaint_themes.csv <- generated at runtime (20 products x 3 themes)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Limitations

- Sentiment scoring covers 100K of 481K total reviews — products with low representation in the scored sample may be under-analyzed; full coverage would require additional compute time
- DistilBERT is a general-purpose sentiment model not fine-tuned on product review language; domain-specific expressions such as "runs small" or "packaging damaged" may not be scored with full accuracy
- Divergence thresholds (HIGH >= 20%, MEDIUM >= 10%) are analytically defined, not calibrated against real return rates or complaint data; a production deployment would validate these against actual business outcomes
- divergence_analysis.py contains a partially hardcoded BigQuery table path — if reproducing with a different GCP project, update the project ID in the SQL query on line 47
- Analysis covers the All_Beauty category only — divergence patterns likely differ across categories with different purchase motivations and review cultures
- Gemini complaint extraction processes the top 20 flagged products only; full coverage of all 52 HIGH + MEDIUM RISK products would require additional API quota

---

## Stack

Python · Google BigQuery · HuggingFace Datasets · DistilBERT · Gemini 2.5 Flash · google-genai · pandas · matplotlib · Jupyter
