# Complete Setup Guide - All Phases

## Prerequisites

- Python 3.9+
- Docker Desktop
- Git
- Astro CLI (for Airflow)
- DagsHub account
- Docker Hub account
- GitHub account

---

## PHASE I: Data Ingestion Setup

### Step 1: DagsHub Setup (Already Done)

Your repo: https://dagshub.com/i222515/mandi

### Step 2: DVC Setup (Already Done)

\`\`\`bash
python -m dvc init
python -m dvc remote add -d dagshub https://dagshub.com/i222515/mandi.dvc
python -m dvc remote modify dagshub --local auth basic
python -m dvc remote modify dagshub --local user i222515
python -m dvc remote modify dagshub --local password YOUR_TOKEN
\`\`\`

### Step 3: Track Data

\`\`\`bash
python -m dvc add include/processed_combined.csv
python -m dvc add data/
python -m dvc add models/
git add *.dvc .gitignore
git commit -m "Track data with DVC"
python -m dvc push
git push origin main
\`\`\`

### Step 4: Run Airflow DAG

\`\`\`bash
astro dev start
# Go to http://localhost:8080
# Enable and trigger mandi_automation DAG
\`\`\`

---

## PHASE II: Model Training Setup

### Step 1: Set Environment Variables

Create `.env` file:

\`\`\`
DAGSHUB_USERNAME=i222515
DAGSHUB_TOKEN=your_token
MLFLOW_TRACKING_URI=https://dagshub.com/i222515/mandi.mlflow
DATA_PATH=include/processed_combined.csv
MODEL_PATH=models/mandi_pipeline.pkl
\`\`\`

### Step 2: Train Model

\`\`\`bash
python scripts/train_model.py
\`\`\`

### Step 3: Verify on DagsHub

Go to: https://dagshub.com/i222515/mandi/experiments

---

## PHASE III: CI/CD Setup

### Step 1: Create GitHub Repository

1. Go to GitHub and create new repo
2. Push your code:

\`\`\`bash
git remote add github https://github.com/YOUR_USERNAME/mandi.git
git push -u github main
\`\`\`

### Step 2: Create Branches

\`\`\`bash
git checkout -b dev
git push -u github dev

git checkout -b test
git push -u github test

git checkout main
\`\`\`

### Step 3: Add GitHub Secrets

Go to: GitHub Repo > Settings > Secrets and Variables > Actions

Add these secrets:
- `DAGSHUB_USERNAME`: i222515
- `DAGSHUB_TOKEN`: your_token
- `MLFLOW_TRACKING_URI`: https://dagshub.com/i222515/mandi.mlflow
- `DOCKER_USERNAME`: your_dockerhub_username
- `DOCKER_PASSWORD`: your_dockerhub_password

### Step 4: Enable Branch Protection

Go to: Settings > Branches > Add rule

For `test` and `master` branches:
- [x] Require pull request reviews before merging
- [x] Require status checks to pass

### Step 5: Test CI/CD

\`\`\`bash
# Create feature branch
git checkout -b feature/test-ci
echo "# Test" >> test.md
git add test.md
git commit -m "Test CI"
git push -u github feature/test-ci

# Create PR to dev branch on GitHub
# Watch CI run!
\`\`\`

---

## PHASE IV: Monitoring Setup

### Step 1: Start All Services

\`\`\`bash
docker-compose up -d
\`\`\`

### Step 2: Verify Services

| Service | URL | Credentials |
|---------|-----|-------------|
| API | http://localhost:8000 | - |
| API Health | http://localhost:8000/health | - |
| API Metrics | http://localhost:8000/metrics | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3001 | admin/admin |

### Step 3: Setup Grafana Dashboard

1. Go to http://localhost:3001
2. Login: admin / admin
3. Go to: Dashboards > Import
4. Upload: `grafana/dashboards/mandi-dashboard.json`

### Step 4: Test Prediction API

\`\`\`bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"city": "Lahore", "crop": "Onion", "date": "2024-01-15"}'
\`\`\`

### Step 5: View Metrics

1. Make several API calls
2. Go to Prometheus: http://localhost:9090
3. Query: `mandi_api_requests_total`
4. View in Grafana dashboard

---

## Verification Checklist

### Phase I
- [ ] DagsHub repo created
- [ ] DVC initialized and data pushed
- [ ] Airflow DAG runs successfully
- [ ] MLflow experiments visible on DagsHub

### Phase II
- [ ] Model trained successfully
- [ ] Metrics logged to MLflow
- [ ] Model registered in Model Registry

### Phase III
- [ ] GitHub Actions workflows created
- [ ] CI runs on PR to dev
- [ ] CML reports generated on PR to test
- [ ] Docker image built and pushed

### Phase IV
- [ ] API running with /metrics endpoint
- [ ] Prometheus collecting metrics
- [ ] Grafana dashboard showing data
- [ ] Alerts configured

---

## Troubleshooting

### DVC Push Fails
\`\`\`bash
python -m dvc remote modify dagshub --local auth basic
python -m dvc remote modify dagshub --local user i222515
python -m dvc remote modify dagshub --local password YOUR_TOKEN
\`\`\`

### MLflow 403 Error
Ensure environment variables are set:
\`\`\`bash
export MLFLOW_TRACKING_USERNAME=i222515
export MLFLOW_TRACKING_PASSWORD=YOUR_TOKEN
\`\`\`

### Docker Build Fails
\`\`\`bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
\`\`\`

### Airflow Connection Error
\`\`\`bash
astro dev stop
astro dev start
