# Mandi Price Prediction - MLOps Pipeline

A complete MLOps pipeline for predicting agricultural commodity prices in Pakistan using real-time data from AMIS.

## Project Structure

\`\`\`
mandi/
├── dags/                    # Airflow DAGs
│   └── mandi_automation.py  # Main ETL pipeline
├── api/                     # FastAPI prediction service
│   ├── app.py               # API with Prometheus metrics
│   └── Dockerfile           # Container definition
├── scripts/                 # Training and utility scripts
│   ├── train_model.py       # Model training with MLflow
│   └── generate_cml_report.py
├── tests/                   # Unit tests
├── grafana/                 # Monitoring dashboards
├── .github/workflows/       # CI/CD pipelines
├── docker-compose.yaml      # Local services
└── prometheus.yml           # Metrics configuration
\`\`\`

## Phase Checklist

### Phase I: Data Ingestion
- [x] Airflow DAG with daily schedule
- [x] Data extraction from AMIS API
- [x] Quality gate (null check, validation)
- [x] Feature engineering (lag, time features)
- [x] DVC data versioning
- [x] DagsHub integration
- [x] Pandas profiling reports
- [x] MLflow experiment logging

### Phase II: Experimentation & Model Management
- [x] train.py with MLflow tracking
- [x] Hyperparameter logging
- [x] Metrics logging (RMSE, MAE, R2)
- [x] Model artifact storage
- [x] MLflow Model Registry

### Phase III: CI/CD
- [x] GitHub Actions workflows
- [x] Code quality checks (linting)
- [x] Unit tests
- [x] CML reports for PRs
- [x] Docker containerization
- [x] Docker Hub push
- [x] Deployment verification

### Phase IV: Monitoring & Observability
- [x] Prometheus metrics in API
- [x] Request latency tracking
- [x] Data drift detection
- [x] Grafana dashboards
- [x] Alerting rules

## Quick Start

### 1. Clone and Setup

\`\`\`bash
git clone https://dagshub.com/i222515/mandi.git
cd mandi

# Create .env file
cp .env.example .env
# Edit .env with your credentials
\`\`\`

### 2. Initialize DVC

\`\`\`bash
python -m dvc init
python -m dvc remote add -d dagshub https://dagshub.com/i222515/mandi.dvc
python -m dvc remote modify dagshub --local auth basic
python -m dvc remote modify dagshub --local user YOUR_USERNAME
python -m dvc remote modify dagshub --local password YOUR_TOKEN
python -m dvc pull
\`\`\`

### 3. Start Airflow (using Astro CLI)

\`\`\`bash
astro dev start
# Access at http://localhost:8080
\`\`\`

### 4. Start Monitoring Stack

\`\`\`bash
docker-compose up -d
\`\`\`

| Service | URL |
|---------|-----|
| API | http://localhost:8000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 (admin/admin) |
| MinIO | http://localhost:9002 (minioadmin/minioadmin) |

### 5. Run Training

\`\`\`bash
python scripts/train_model.py
\`\`\`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API info |
| `/health` | GET | Health check |
| `/predict` | POST | Make prediction |
| `/metrics` | GET | Prometheus metrics |
| `/drift` | GET | Drift statistics |

### Example Prediction

\`\`\`bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"city": "Lahore", "crop": "Onion", "date": "2024-01-15"}'
\`\`\`

## GitHub Secrets Required

Add these in your GitHub repo Settings > Secrets:

| Secret | Description |
|--------|-------------|
| `DAGSHUB_USERNAME` | Your DagsHub username |
| `DAGSHUB_TOKEN` | Your DagsHub access token |
| `MLFLOW_TRACKING_URI` | MLflow tracking URL |
| `DOCKER_USERNAME` | Docker Hub username |
| `DOCKER_PASSWORD` | Docker Hub password |

## Branch Strategy

\`\`\`
feature/* → dev → test → master
\`\`\`

- `feature/*` → `dev`: Code quality + unit tests
- `dev` → `test`: Model training + CML report
- `test` → `master`: Docker build + deploy verification

## Monitoring Alerts

| Alert | Threshold | Severity |
|-------|-----------|----------|
| High Latency | p95 > 500ms | Warning |
| Data Drift | ratio > 30% | Critical |

## License

MIT
