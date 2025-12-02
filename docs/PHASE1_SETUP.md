# Phase 1 Setup Guide

## Overview
This guide covers the complete setup for Phase 1: Problem Definition and Data Ingestion.

## Prerequisites
- Docker & Docker Compose
- Git
- Python 3.9+
- Astronomer CLI (for Airflow)

## Step 1: DagShub Setup (MLflow + DVC Hub)

1. Create account at [dagshub.com](https://dagshub.com)
2. Create new repository named `mandi`
3. Get your token from Settings > Tokens
4. Copy `.env.example` to `.env` and fill in your credentials:

\`\`\`bash
cp .env.example .env
# Edit .env with your DagShub credentials
\`\`\`

## Step 2: Initialize DVC

\`\`\`bash
# Initialize DVC in project root
dvc init

# Add DagShub as remote
dvc remote add -d dagshub s3://mandi-data
dvc remote modify dagshub endpointurl https://dagshub.com/YOUR_USERNAME/mandi.s3
dvc remote modify dagshub access_key_id YOUR_DAGSHUB_TOKEN
dvc remote modify dagshub secret_access_key YOUR_DAGSHUB_TOKEN

# Or use local MinIO
dvc remote add minio s3://mandi-data
dvc remote modify minio endpointurl http://localhost:9000
dvc remote modify minio access_key_id minioadmin
dvc remote modify minio secret_access_key minioadmin
\`\`\`

## Step 3: Start Infrastructure

\`\`\`bash
# Start MinIO, Prometheus, Grafana
docker-compose up -d

# Verify MinIO is running
curl http://localhost:9000/minio/health/live
\`\`\`

## Step 4: Configure MLflow

\`\`\`bash
# Set environment variables
export MLFLOW_TRACKING_URI=https://dagshub.com/YOUR_USERNAME/mandi.mlflow
export MLFLOW_TRACKING_USERNAME=YOUR_USERNAME
export MLFLOW_TRACKING_PASSWORD=YOUR_TOKEN
\`\`\`

## Step 5: Start Airflow

\`\`\`bash
# Using Astronomer CLI
astro dev start

# Airflow UI: http://localhost:8080
# Default credentials: admin/admin
\`\`\`

## Step 6: Create Required Folders

\`\`\`bash
mkdir -p include/raw_history
mkdir -p include/reports
mkdir -p data/raw_history
mkdir -p data/processed
mkdir -p models
\`\`\`

## Step 7: Run the DAG

1. Open Airflow UI at http://localhost:8080
2. Enable the `mandi_automation` DAG
3. Trigger manually or wait for scheduled run

## Verification Checklist

- [ ] MinIO UI accessible at http://localhost:9001
- [ ] Airflow UI accessible at http://localhost:8080
- [ ] DAG `mandi_automation` visible and enabled
- [ ] Raw data saved to `include/raw_history/`
- [ ] Quality gate logs show pass/fail status
- [ ] Profiling report generated in `include/reports/`
- [ ] MLflow experiment visible at DagShub
- [ ] DVC tracking file created (`.dvc` file)

## Folder Structure

\`\`\`
mandi/
├── .dvc/                    # DVC configuration
├── .env                     # Environment variables
├── dags/
│   └── mandi_automation.py  # Main Airflow DAG
├── data/
│   ├── raw_history/         # Raw CSV files (date-stamped)
│   └── processed/           # Processed datasets
├── include/
│   ├── processed_combined.csv
│   ├── raw_history/
│   └── reports/             # Profiling reports
├── models/
│   └── mandi_pipeline.pkl   # Trained model
├── docker-compose.yaml
└── requirements.txt
\`\`\`

## Troubleshooting

### DVC Push Fails
\`\`\`bash
# Check remote configuration
dvc remote list
dvc config --list

# Test connection
dvc push -v
\`\`\`

### MLflow Connection Error
\`\`\`bash
# Verify environment variables
echo $MLFLOW_TRACKING_URI
python -c "import mlflow; print(mlflow.get_tracking_uri())"
\`\`\`

### Quality Gate Fails
- Check the Airflow task logs
- Verify data source is returning valid data
- Adjust null threshold if needed (currently 1%)
\`\`\`

```dvc file="include/processed_combined.csv.dvc"
outs:
- md5: null
  size: null
  hash: md5
  path: processed_combined.csv
