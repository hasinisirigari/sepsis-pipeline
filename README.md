# Sepsis Early Warning System

Real-time sepsis prediction pipeline using MIMIC-IV clinical data. Detects sepsis up to 6 hours before clinical recognition using a LightGBM model (AUROC 0.86) deployed on AWS Lambda.

## Architecture

```
MIMIC-IV CSVs → S3 Bronze (Parquet) → Databricks Silver → Feature Engineering → Gold Training Data
                                                                                       ↓
                                              Kafka Streaming ← Silver Vitals    LightGBM Model
                                                     ↓                                 ↓
                                              Real-time Scoring              AWS Lambda API
                                                     ↓                                 ↓
                                              Clinical Dashboard         Risk Score + SHAP Explanation
```

**Stack:** Python, AWS (S3, Lambda), Databricks (Spark), Confluent Kafka, LightGBM, SHAP, Docker

## Results

| Metric | Value |
|--------|-------|
| AUROC | 0.8588 |
| AUPRC | 0.6975 |
| Recall @ 0.5 threshold | 70.3% |
| Precision @ 0.5 threshold | 62.7% |
| Training data | 942K rows, 39 features, 94K ICU stays |
| Scoring frequency | Every 15 minutes per patient |

**Alert thresholds:** GREEN (<0.3), YELLOW (0.3–0.6), ORANGE (0.6–0.8), RED (≥0.8)

### Top Predictive Features (SHAP)

1. **Hours in ICU** — sepsis risk is strongly time-dependent, peaking in first 24–48h
2. **Temperature (4h mean)** — sustained fever is a hallmark of infection
3. **Heart rate (1h mean)** — tachycardia is an early sepsis indicator
4. **Vasopressor use** — indicates cardiovascular compromise
5. **Respiratory rate (4h mean)** — tachypnea signals respiratory compensation

## Data Pipeline

### Medallion Architecture (Bronze → Silver → Gold)

**Bronze Layer** — Raw MIMIC-IV ingested as Parquet on S3. 10 tables, 600M+ rows including 433M chartevents.

**Silver Layer** — Cleaned and transformed on Databricks:
- 53.7M vital sign rows (filtered from 433M chartevents, 0.1% outliers removed)
- 14.9M lab rows (lactate, creatinine, platelets, bilirubin, WBC)
- 840K vasopressor administrations, 761K antibiotic prescriptions, 811K blood cultures
- 24.9M feature windows across 94K ICU stays

**Gold Layer** — Labeled training data with 39 features per window:
- Current vitals + 1h/4h rolling means (captures trends)
- Lab values (merged via `merge_asof` — last known value at each window)
- Derived features: MAP, Shock Index (HR/SBP), vasopressor flag, hours in ICU
- Sepsis-3 labels: antibiotic + blood culture within 72h, onset during ICU stay

### Feature Engineering

Each 15-minute window computes features using vectorized `merge_asof` operations — processing 94K stays in 26 minutes (vs. 8+ hours with row-by-row iteration).

**Why 15-minute windows?** Matches ICU charting frequency. Granular enough to catch rapid deterioration while avoiding redundant predictions.

**Why `merge_asof`?** Aligns irregular clinical measurements to regular scoring windows without resampling. A heart rate at 10:00 AM correctly maps to windows at 10:00, 10:15, 10:30 until a new reading arrives. No artificial forward-fill data points.

## Streaming Layer

Confluent Kafka replays MIMIC-IV data as a real-time stream:
- 3 topics: `patient.vitals`, `patient.labs`, `patient.medications` (3 partitions each)
- Messages keyed by `stay_id` for per-patient ordering guarantees
- Configurable replay speed (288x = 24h of data in 5 minutes)

## Model

**LightGBM** chosen for:
- Native null handling (clinical data is inherently sparse — temperature is charted every 4–8h, not every 15 min)
- Fast inference (~10ms) suitable for real-time scoring on Lambda
- Interpretable with SHAP TreeExplainer

**Training approach:**
- Patient-level train/test split (80/20) to prevent data leakage
- `scale_pos_weight` for class imbalance (0.95% positive rate in raw data)
- Stratified sampling: all positive windows + 3x negative downsampling

**Sepsis-3 labeling:** Suspected infection (antibiotic + blood culture within 72h) with onset during ICU stay → 16,644 sepsis cases out of 94,458 stays (17.6% prevalence).

## Deployment

AWS Lambda serves predictions via REST API:
- LightGBM native Booster format (120 KB model)
- Lambda Layer with LightGBM + NumPy + libgomp (~40 MB)
- Cold start ~3s, warm inference ~10ms
- Returns: sepsis probability, alert level (GREEN/YELLOW/ORANGE/RED), contributing features

**Sample request:**
```json
{
  "stay_id": 12345,
  "heart_rate_current": 112,
  "sbp_noninvasive_current": 85,
  "temperature_current": 38.9,
  "lactate": 4.2,
  "on_vasopressor": 1,
  "hours_in_icu": 14.5
}
```

**Sample response:**
```json
{
  "sepsis_probability": 0.6353,
  "alert_level": "ORANGE",
  "alert_message": "High risk - close monitoring and reassessment needed",
  "patient_id": 12345,
  "features_received": 25,
  "features_expected": 39
}
```

## Dashboard

A monitoring dashboard prototype is available in `dashboard/SepsisDashboard.jsx` — displays patient risk scores, vital signs, lab values, trend sparklines, and SHAP feature importance for each patient.

## Project Structure

```
sepsis-pipeline/
├── docker/
│   └── docker-compose.yml          # Kafka, Zookeeper, PostgreSQL
├── sql/gold/
│   └── create_omop_tables.sql      # OMOP-aligned Gold schema
├── src/
│   ├── ingestion/
│   │   ├── bronze_ingestion.py     # CSV → Parquet → S3
│   │   ├── kafka_producer.py       # Replay MIMIC-IV to Kafka
│   │   └── kafka_consumer.py       # Consume and verify messages
│   ├── serving/
│   │   ├── lambda_handler.py       # Lambda inference function
│   │   └── test_lambda.py          # Local testing
│   ├── training/
│   │   └── train_model.py          # LightGBM training + SHAP
│   └── utils/
│       ├── config.py               # Centralized configuration
│       ├── logging_config.py       # Structured logging
│       └── s3_utils.py             # S3 client utilities
├── notebooks/
│   ├── 01_bronze_to_silver.py      # Databricks: Silver transforms
│   ├── 02_feature_engineering.py   # Databricks: 24.9M feature windows
│   └── 03_labeling_and_training.py # Databricks: Sepsis-3 labels + Gold data
├── models/
│   ├── lgbm_sepsis_model.pkl       # Trained model (sklearn wrapper)
│   ├── lgbm_sepsis_model.txt       # Native LightGBM format (for Lambda)
│   ├── metrics.json                # Performance metrics
│   └── shap_importance.csv         # Feature importance rankings
├── scripts/
│   └── create_kafka_topics.py      # Kafka topic setup
├── dashboard/
│   └── SepsisDashboard.jsx         # Monitoring dashboard prototype
└── README.md
```

## Setup

### Prerequisites
- Python 3.10+, Docker, AWS CLI
- Databricks account (Free Edition)
- MIMIC-IV access via PhysioNet

### Quick Start
```bash
conda create -n sepsis python=3.10
conda activate sepsis
pip install -r requirements.txt

# Start local services
docker compose -f docker/docker-compose.yml up -d

# Create Kafka topics
python scripts/create_kafka_topics.py

# Train model (requires Gold data on S3)
python -m src.training.train_model

# Test Lambda locally
python -m src.serving.test_lambda

# Replay data to Kafka
python -m src.ingestion.kafka_producer --speed 288 --stays 100
```

## Key Technical Decisions

| Decision | Rationale |
|----------|-----------|
| Parquet over CSV | 39 GB → ~10 GB, columnar reads, type preservation |
| boto3 on Databricks | Serverless compute blocks `fs.s3a` Spark config |
| `merge_asof` for features | 17x faster than iterative approach (26 min vs 8+ hours) |
| 7-day cap per stay | Sepsis typically develops in first 48–72h; avoids wasted compute on 121-day stays |
| LightGBM over XGBoost | Native null handling critical for sparse clinical data |
| Lambda over SageMaker | Demonstrates infrastructure knowledge; 200 req/hour doesn't justify managed endpoint |
| Patient-level splits | Prevents temporal leakage between train/test sets |

## Data

This project uses [MIMIC-IV](https://physionet.org/content/mimiciv/), a freely available critical care database. Access requires PhysioNet credentialed researcher status and completion of CITI training.

### Citations

Johnson, A., Bulgarelli, L., Pollard, T., Gow, B., Moody, B., Horng, S., Celi, L. A., & Mark, R. (2024). MIMIC-IV (version 3.1). PhysioNet. https://doi.org/10.13026/kpb9-mt58

Johnson, A.E.W., Bulgarelli, L., Shen, L. et al. MIMIC-IV, a freely accessible electronic health record dataset. Sci Data 10, 1 (2023). https://doi.org/10.1038/s41597-022-01899-x

Goldberger, A., Amaral, L., Glass, L., Hausdorff, J., Ivanov, P. C., Mark, R., ... & Stanley, H. E. (2000). PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research resource for complex physiologic signals. Circulation [Online]. 101 (23), pp. e215–e220.
