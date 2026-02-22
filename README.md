# Real-Time Sepsis Early Warning System

A production-grade, end-to-end data engineering + ML pipeline that replays ICU patient data through a streaming architecture, computes rolling clinical risk features, scores sepsis risk every 15 minutes, and serves live alerts through a Grafana dashboard with LLM-generated clinical summaries.

## Clinical Motivation

Sepsis kills 270,000 Americans annually and is the #1 cause of in-hospital deaths. Every hour of delayed detection increases mortality by ~7%. This pipeline simulates what a real-time clinical decision support system would look like in a hospital setting.

## Architecture
```
MIMIC-IV --> Kafka --> Spark Streaming --> Delta Lake (S3) --> LightGBM --> PostgreSQL --> Grafana
                           |                    |                              |
                      (Databricks)         (Silver Layer)                (Gold/OMOP)
                           |                    |
                    Great Expectations       MLflow
                    (Data Quality)      (Experiment Tracking)
```

Multi-cloud: BigQuery (exploration) | Databricks (Spark processing) | AWS (storage, serving, orchestration)

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | MIMIC-IV v3.1 | ICU patient records (PhysioNet) |
| Exploration | GCP BigQuery | Schema validation, Sepsis-3 label validation |
| Streaming | Apache Kafka (Docker) | Real-time message queue |
| Processing | PySpark on Databricks | Bronze to Silver transforms, feature engineering |
| Storage | AWS S3 + Delta Lake | Medallion data lake (Bronze/Silver) |
| Gold Layer | PostgreSQL (RDS) | OMOP CDM-aligned serving tables |
| Data Quality | Great Expectations | Automated validation on Silver layer |
| ML Model | LightGBM + MLflow + SHAP | Sepsis risk prediction with explainability |
| API | FastAPI on AWS Lambda | RESTful endpoints for risk scores |
| Dashboard | Grafana | Live ICU ward overview |
| LLM Alerts | Amazon Bedrock | Plain-English clinical summaries |
| Orchestration | Apache Airflow | DAG-based pipeline scheduling |
| CI/CD | GitHub Actions | Automated testing and deployment |

## Project Status

- [x] Phase 1: Project setup and BigQuery exploration
- [ ] Phase 2: Local pipeline foundation (Docker, Bronze ingestion)
- [ ] Phase 3: Databricks Silver pipeline
- [ ] Phase 4: Kafka streaming layer
- [ ] Phase 5: ML model training
- [ ] Phase 6: AWS deployment
- [ ] Phase 7: Dashboard, alerts, polish

## Quick Start
```bash
git clone https://github.com/hasinisirigari/sepsis-pipeline.git
cd sepsis-pipeline
conda create -n sepsis python=3.11 -y
conda activate sepsis
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

---

*Data source: MIMIC-IV v3.1 (PhysioNet, credentialed access required)*
```
